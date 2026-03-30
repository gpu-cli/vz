#!/usr/bin/env bash
# Install script for vz — macOS VM sandbox for coding agents.
#
# Usage:
#   curl -sSf https://raw.githubusercontent.com/gpu-cli/vz/main/scripts/install.sh | sh
#
# Environment variables:
#   VZ_VERSION    — Install a specific version (e.g., "0.1.0"). Default: latest.
#   VZ_INSTALL_DIR — Installation directory. Default: ~/.vz
#   VZ_NO_LINUX   — Set to "1" to skip Linux kernel/initramfs download.

set -euo pipefail

REPO="gpu-cli/vz"
INSTALL_DIR="${VZ_INSTALL_DIR:-$HOME/.vz}"
BIN_DIR="$INSTALL_DIR/bin"
LINUX_DIR="$INSTALL_DIR/linux"

# --- Preflight checks ---

check_platform() {
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"

    if [ "$os" != "Darwin" ]; then
        echo "error: vz requires macOS (Virtualization.framework is macOS-only)." >&2
        echo "       Detected: $os" >&2
        exit 1
    fi

    if [ "$arch" != "arm64" ]; then
        echo "error: vz requires Apple Silicon (arm64)." >&2
        echo "       Detected: $arch" >&2
        exit 1
    fi
}

check_dependencies() {
    for cmd in curl shasum tar; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            echo "error: required command not found: $cmd" >&2
            exit 1
        fi
    done
}

# --- Version resolution ---

resolve_version() {
    if [ -n "${VZ_VERSION:-}" ]; then
        echo "$VZ_VERSION"
        return
    fi

    local latest
    latest="$(curl -sSf -o /dev/null -w '%{redirect_url}' \
        "https://github.com/$REPO/releases/latest" 2>/dev/null || true)"

    if [ -z "$latest" ]; then
        echo "error: could not determine latest version from GitHub." >&2
        echo "       Set VZ_VERSION explicitly: VZ_VERSION=0.1.0 sh install.sh" >&2
        exit 1
    fi

    # Extract tag from redirect URL: .../releases/tag/v0.1.0 -> 0.1.0
    local tag="${latest##*/}"
    echo "${tag#v}"
}

# --- Download helpers ---

download() {
    local url="$1" dest="$2"
    echo "  downloading: $url"
    curl -sSfL -o "$dest" "$url"
}

verify_checksum() {
    local file="$1" checksum_file="$2"
    local expected actual

    expected="$(awk '{print $1}' "$checksum_file")"
    actual="$(shasum -a 256 "$file" | awk '{print $1}')"

    if [ "$expected" != "$actual" ]; then
        echo "error: checksum mismatch for $(basename "$file")" >&2
        echo "  expected: $expected" >&2
        echo "  actual:   $actual" >&2
        rm -f "$file" "$checksum_file"
        exit 1
    fi
}

# --- Install steps ---

install_cli() {
    local version="$1"
    local base_url="https://github.com/$REPO/releases/download/v${version}"
    local binary_name="vz-v${version}-darwin-arm64"

    mkdir -p "$BIN_DIR"

    echo "Installing vz v${version}..."

    download "$base_url/$binary_name" "$BIN_DIR/vz"
    download "$base_url/${binary_name}.sha256" "$BIN_DIR/vz.sha256"

    verify_checksum "$BIN_DIR/vz" "$BIN_DIR/vz.sha256"
    rm "$BIN_DIR/vz.sha256"

    chmod +x "$BIN_DIR/vz"

    # The binary is pre-signed with Developer ID + notarized.
    # Verify the signature is intact after download.
    if codesign --verify "$BIN_DIR/vz" 2>/dev/null; then
        echo "  signature verified"
    else
        echo "  warning: signature verification failed — the binary may trigger Gatekeeper."
        echo "  You can ad-hoc sign it: codesign --sign - --force --entitlements <plist> $BIN_DIR/vz"
    fi

    echo "  installed: $BIN_DIR/vz"
}

install_linux_artifacts() {
    local version="$1"

    if [ "${VZ_NO_LINUX:-}" = "1" ]; then
        echo "Skipping Linux artifacts (VZ_NO_LINUX=1)."
        return
    fi

    local base_url="https://github.com/$REPO/releases/download/v${version}"
    local tarball_name="vz-linux-v${version}-arm64.tar.gz"

    mkdir -p "$LINUX_DIR"

    echo "Installing Linux kernel + initramfs..."

    download "$base_url/$tarball_name" "$LINUX_DIR/$tarball_name"
    download "$base_url/${tarball_name}.sha256" "$LINUX_DIR/${tarball_name}.sha256"

    verify_checksum "$LINUX_DIR/$tarball_name" "$LINUX_DIR/${tarball_name}.sha256"
    rm "$LINUX_DIR/${tarball_name}.sha256"

    tar xzf "$LINUX_DIR/$tarball_name" -C "$LINUX_DIR"
    rm "$LINUX_DIR/$tarball_name"

    echo "  installed: $LINUX_DIR/{vmlinux, initramfs.img, youki, version.json}"
}

setup_path() {
    local shell_rc=""

    if [ -n "${ZSH_VERSION:-}" ] || [ "$(basename "${SHELL:-}")" = "zsh" ]; then
        shell_rc="$HOME/.zshrc"
    elif [ -n "${BASH_VERSION:-}" ] || [ "$(basename "${SHELL:-}")" = "bash" ]; then
        shell_rc="$HOME/.bash_profile"
    fi

    # Check if already in PATH
    if echo "$PATH" | tr ':' '\n' | grep -qx "$BIN_DIR"; then
        return
    fi

    local path_line="export PATH=\"$BIN_DIR:\$PATH\""

    if [ -n "$shell_rc" ]; then
        if [ -f "$shell_rc" ] && grep -qF "$BIN_DIR" "$shell_rc" 2>/dev/null; then
            return
        fi

        echo "" >> "$shell_rc"
        echo "# vz" >> "$shell_rc"
        echo "$path_line" >> "$shell_rc"
        echo "  added $BIN_DIR to PATH in $shell_rc"
        echo "  run: source $shell_rc"
    else
        echo "  add to your shell profile: $path_line"
    fi
}

# --- Main ---

main() {
    check_platform
    check_dependencies

    local version
    version="$(resolve_version)"

    install_cli "$version"
    install_linux_artifacts "$version"
    setup_path

    echo ""
    echo "vz v${version} installed successfully!"
    echo ""
    echo "Get started:"
    echo "  vz --help"
    echo "  vz vm linux init --name my-vm"
    echo "  vz vm linux run --name my-vm --guest-command 'uname -a'"
}

main "$@"
