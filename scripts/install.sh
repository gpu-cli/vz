#!/usr/bin/env bash
# Install script for vz — instant sandboxed Linux environments on macOS.
#
# Usage:
#   curl -sSf https://raw.githubusercontent.com/gpu-cli/vz/main/scripts/install.sh | sh
#
# Environment variables:
#   VZ_VERSION     — Install a specific version (e.g., "0.2.0"). Default: latest.
#   VZ_INSTALL_DIR — Installation directory. Default: ~/.vz
#   VZ_NO_LINUX    — Set to "1" to skip Linux kernel/initramfs download.

set -euo pipefail

REPO="gpu-cli/vz"
INSTALL_DIR="${VZ_INSTALL_DIR:-$HOME/.vz}"
BIN_DIR="$INSTALL_DIR/bin"
LINUX_DIR="$INSTALL_DIR/linux"
VERSION_FILE="$INSTALL_DIR/.installed-version"

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
        echo "       Set VZ_VERSION explicitly: VZ_VERSION=0.2.0 sh install.sh" >&2
        exit 1
    fi

    local tag="${latest##*/}"
    echo "${tag#v}"
}

installed_version() {
    if [ -f "$VERSION_FILE" ]; then
        cat "$VERSION_FILE"
    else
        echo ""
    fi
}

# --- Download helpers ---

download() {
    local url="$1" dest="$2"
    echo "  downloading: $(basename "$dest")"
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

install_binary() {
    local version="$1" name="$2"
    local base_url="https://github.com/$REPO/releases/download/v${version}"
    local artifact_name="${name}-v${version}-darwin-arm64"

    download "$base_url/$artifact_name" "$BIN_DIR/$name"
    download "$base_url/${artifact_name}.sha256" "$BIN_DIR/${name}.sha256"

    verify_checksum "$BIN_DIR/$name" "$BIN_DIR/${name}.sha256"
    rm "$BIN_DIR/${name}.sha256"

    chmod +x "$BIN_DIR/$name"

    if codesign --verify "$BIN_DIR/$name" 2>/dev/null; then
        echo "  $name: signature verified"
    else
        echo "  $name: warning — signature verification failed, may trigger Gatekeeper"
    fi
}

install_binaries() {
    local version="$1"

    mkdir -p "$BIN_DIR"

    echo "Installing vz v${version}..."
    install_binary "$version" "vz"
    install_binary "$version" "vz-runtimed"
    install_binary "$version" "vz-guest-agent"
}

install_linux_artifacts() {
    local version="$1"

    if [ "${VZ_NO_LINUX:-}" = "1" ]; then
        echo "Skipping Linux artifacts (VZ_NO_LINUX=1)."
        return
    fi

    local base_url="https://github.com/$REPO/releases/download/v${version}"
    local tarball_name="vz-linux-v${version}-arm64.tar.gz"

    # Check if Linux artifacts are already at this version.
    if [ -f "$LINUX_DIR/version.json" ]; then
        local current_kernel_hash
        current_kernel_hash="$(python3 -c "import json; print(json.load(open('$LINUX_DIR/version.json'))['sha256_vmlinux'])" 2>/dev/null || echo "")"
        if [ -n "$current_kernel_hash" ] && [ -f "$LINUX_DIR/vmlinux" ]; then
            local actual_hash
            actual_hash="$(shasum -a 256 "$LINUX_DIR/vmlinux" | awk '{print $1}')"
            if [ "$current_kernel_hash" = "$actual_hash" ]; then
                echo "Linux artifacts already up to date."
                return
            fi
        fi
    fi

    mkdir -p "$LINUX_DIR"

    echo "Installing Linux kernel + initramfs..."

    download "$base_url/$tarball_name" "$LINUX_DIR/$tarball_name"
    download "$base_url/${tarball_name}.sha256" "$LINUX_DIR/${tarball_name}.sha256"

    verify_checksum "$LINUX_DIR/$tarball_name" "$LINUX_DIR/${tarball_name}.sha256"
    rm "$LINUX_DIR/${tarball_name}.sha256"

    tar xzf "$LINUX_DIR/$tarball_name" -C "$LINUX_DIR"
    rm "$LINUX_DIR/$tarball_name"

    echo "  installed: vmlinux, initramfs.img, youki, version.json"
}

setup_path() {
    local shell_rc=""

    if [ -n "${ZSH_VERSION:-}" ] || [ "$(basename "${SHELL:-}")" = "zsh" ]; then
        shell_rc="$HOME/.zshrc"
    elif [ -n "${BASH_VERSION:-}" ] || [ "$(basename "${SHELL:-}")" = "bash" ]; then
        shell_rc="$HOME/.bash_profile"
    fi

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

    local prev_version
    prev_version="$(installed_version)"

    if [ -n "$prev_version" ] && [ "$prev_version" = "$version" ]; then
        echo "vz v${version} is already installed."
        echo "Re-installing..."
    elif [ -n "$prev_version" ]; then
        echo "Upgrading vz from v${prev_version} to v${version}..."
    fi

    # Stop running daemon before upgrading binaries.
    if [ -n "$prev_version" ] && command -v "$BIN_DIR/vz-runtimed" >/dev/null 2>&1; then
        pkill -f "$BIN_DIR/vz-runtimed" 2>/dev/null || true
        sleep 1
    fi

    install_binaries "$version"
    install_linux_artifacts "$version"
    setup_path

    echo "$version" > "$VERSION_FILE"

    echo ""
    echo "vz v${version} installed successfully!"
    echo ""
    echo "Get started:"
    echo "  cd your-project"
    echo "  cat > vz.json << 'EOF'"
    echo '  {'
    echo '    "image": "ubuntu:24.04",'
    echo '    "workspace": "/workspace",'
    echo '    "mounts": [{ "source": ".", "target": "/workspace" }]'
    echo '  }'
    echo "  EOF"
    echo "  vz run echo hello"
}

main "$@"
