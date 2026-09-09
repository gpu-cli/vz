#!/usr/bin/env bash
# Install script for vz — Developer Environments on Apple-silicon macOS.
#
# Usage:
#   curl -sSf https://raw.githubusercontent.com/gpu-cli/vz/main/scripts/install.sh | sh
#
# Environment variables:
#   VZ_VERSION     — Install a specific version (e.g., "0.2.0"). Default: latest.
#   VZ_INSTALL_DIR — Installation directory. Default: ~/.vz
#   VZ_NO_LINUX    — Set to "1" to skip Linux kernel/initramfs download.
#   VZ_NATIVE_BUNDLE — Optional canonical DEV macOS bundle directory.
#   VZ_NATIVE_MANIFEST_SHA256 — Trusted manifest pin required with VZ_NATIVE_BUNDLE.
#   VZ_LINUX_PROFILE — Linux profile to install: all, developer, or container. Default: all.

set -euo pipefail

REPO="gpu-cli/vz"
INSTALL_DIR="${VZ_INSTALL_DIR:-$HOME/.vz}"
BIN_DIR="$INSTALL_DIR/bin"
LINUX_DIR="$INSTALL_DIR/linux"
VERSION_FILE="$INSTALL_DIR/.installed-version"
INSTALLED_LINUX_PROFILES=()

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

    if [ -n "${VZ_LOCAL_RELEASE_DIR:-}" ]; then
        /usr/bin/plutil -extract release_version raw -expect string \
            "$VZ_LOCAL_RELEASE_DIR/release-manifest.json" 2>/dev/null && return
        echo "error: $VZ_LOCAL_RELEASE_DIR/release-manifest.json has no release_version." >&2
        exit 1
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

download_if_available() {
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

# Digest of one path relative to a local release directory, read from the
# checksums.sha256 that directory ships. An unlisted path is refused: an
# unverified artifact must never be installed.
local_release_digest() {
    local relpath="$1" line
    line="$(/usr/bin/grep -E "^[0-9a-f]{64}  ${relpath}\$" \
        "$VZ_LOCAL_RELEASE_DIR/checksums.sha256")" || {
        echo "error: $relpath is not listed in $VZ_LOCAL_RELEASE_DIR/checksums.sha256" >&2
        exit 1
    }
    printf '%s' "${line%% *}"
}

install_local_file() {
    local relpath="$1" dest="$2" expected actual
    expected="$(local_release_digest "$relpath")"
    cp "$VZ_LOCAL_RELEASE_DIR/$relpath" "$dest"
    actual="$(shasum -a 256 "$dest" | awk '{print $1}')"
    if [ "$expected" != "$actual" ]; then
        echo "error: checksum mismatch for $relpath" >&2
        echo "  expected: $expected" >&2
        echo "  actual:   $actual" >&2
        rm -f "$dest"
        exit 1
    fi
}

install_binary() {
    local version="$1" name="$2"

    if [ -n "${VZ_LOCAL_RELEASE_DIR:-}" ]; then
        echo "  staging: $name"
        install_local_file "bin/$name" "$BIN_DIR/$name"
        chmod +x "$BIN_DIR/$name"
        if codesign --verify "$BIN_DIR/$name" 2>/dev/null; then
            echo "  $name: signature verified"
        else
            echo "  $name: warning — signature verification failed, may trigger Gatekeeper"
        fi
        return
    fi

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
    install_binary "$version" "vz-agent-loader"
    install_binary "$version" "vz-macos-setup"
}

install_linux_artifacts() {
    local version="$1"

    if [ "${VZ_NO_LINUX:-}" = "1" ]; then
        echo "Skipping Linux artifacts (VZ_NO_LINUX=1)."
        return
    fi

    local profile="${VZ_LINUX_PROFILE:-all}"
    case "$profile" in
        all)
            install_linux_profile_artifacts "$version" "developer" "$LINUX_DIR/developer"
            copy_linux_profile_to_legacy_default "$LINUX_DIR/developer"
            install_linux_profile_artifacts "$version" "container" "$LINUX_DIR/container" "optional"
            ;;
        developer)
            install_linux_profile_artifacts "$version" "developer" "$LINUX_DIR/developer"
            copy_linux_profile_to_legacy_default "$LINUX_DIR/developer"
            ;;
        container)
            install_linux_profile_artifacts "$version" "container" "$LINUX_DIR/container"
            ;;
        *)
            echo "error: unsupported VZ_LINUX_PROFILE=$profile (expected all, developer, or container)" >&2
            exit 1
            ;;
    esac

 }

install_machine_catalog() {
    local version="$1"
    if [ "${#INSTALLED_LINUX_PROFILES[@]}" = 0 ] && [ -z "${VZ_NATIVE_BUNDLE:-}" ]; then
        return
    fi
    # Only profiles downloaded by this transaction are catalogued. In particular,
    # an unavailable optional profile must not adopt an older directory on disk.
    local catalog_args=()
    local installed_profile
    for installed_profile in "${INSTALLED_LINUX_PROFILES[@]}"; do
        catalog_args+=(--installed-linux-profile "$installed_profile")
    done
    if [ -n "${VZ_NATIVE_BUNDLE:-}" ]; then
        : "${VZ_NATIVE_MANIFEST_SHA256:?VZ_NATIVE_MANIFEST_SHA256 is required for a DEV native bundle}"
        catalog_args+=(--installed-native-bundle "$VZ_NATIVE_BUNDLE"
            --installed-native-manifest-sha256 "$VZ_NATIVE_MANIFEST_SHA256")
    fi
    local canonical_prefix
    canonical_prefix="$(cd "$INSTALL_DIR" && pwd -P)"
    "$BIN_DIR/vz-runtimed" --write-installed-machine-target-catalog "$canonical_prefix" \
        --installed-release-version "$version" "${catalog_args[@]}"
}

install_linux_profile_artifacts() {
    local version="$1"
    local profile="$2"
    local dest_dir="$3"
    local required="${4:-required}"
    local base_url="https://github.com/$REPO/releases/download/v${version}"
    local tarball_name="vz-linux-${profile}-v${version}-arm64.tar.gz"

    mkdir -p "$dest_dir"

    echo "Installing Linux ${profile} kernel + initramfs..."

    if [ -n "${VZ_LOCAL_RELEASE_DIR:-}" ]; then
        local artifact
        if [ ! -d "$VZ_LOCAL_RELEASE_DIR/linux/$profile" ]; then
            if [ "$required" = "optional" ]; then
                echo "  warning: this release provides no ${profile} Linux artifacts; skipping"
                return
            fi
            echo "error: this release provides no ${profile} Linux artifacts" >&2
            exit 1
        fi
        for artifact in $(cd "$VZ_LOCAL_RELEASE_DIR/linux/$profile" && ls); do
            install_local_file "linux/$profile/$artifact" "$dest_dir/$artifact"
        done
        echo "  installed ${profile}: $dest_dir"
        INSTALLED_LINUX_PROFILES+=("$profile")
        return
    fi

    if ! download_if_available "$base_url/$tarball_name" "$dest_dir/$tarball_name"; then
        rm -f "$dest_dir/$tarball_name"
        if [ "$profile" != "developer" ]; then
            if [ "$required" = "optional" ]; then
                echo "  warning: release v${version} does not provide ${profile} Linux artifacts; skipping"
                return
            fi
            echo "error: release v${version} does not provide ${profile} Linux artifacts" >&2
            exit 1
        fi
        tarball_name="vz-linux-v${version}-arm64.tar.gz"
        echo "  falling back to legacy developer artifact"
        download "$base_url/$tarball_name" "$dest_dir/$tarball_name"
    fi

    download "$base_url/${tarball_name}.sha256" "$dest_dir/${tarball_name}.sha256"

    verify_checksum "$dest_dir/$tarball_name" "$dest_dir/${tarball_name}.sha256"
    rm "$dest_dir/${tarball_name}.sha256"

    tar xzf "$dest_dir/$tarball_name" -C "$dest_dir"
    rm "$dest_dir/$tarball_name"

    echo "  installed ${profile}: $dest_dir"
    INSTALLED_LINUX_PROFILES+=("$profile")
}

legacy_developer_probe_sha() {
    local metadata="$1" probe_type profile archive schema digest parsed
    [ -f "$metadata" ] && [ ! -L "$metadata" ] || return 1
    # plutil's plist-only lint rejects JSON on some macOS releases. Conversion
    # to stdout validates JSON (including null) without rewriting the input.
    parsed="$(/usr/bin/plutil -convert json -o - "$metadata")" || return 1
    [[ "$parsed" == \{* ]] || return 1
    profile="$(/usr/bin/plutil -extract profile raw -expect string "$metadata" 2>/dev/null)" || profile=""
    [ -z "$profile" ] || [ "$profile" = developer ] || return 1
    probe_type="$(/usr/bin/plutil -type developer_probe "$metadata" 2>/dev/null)" || return 0
    # plutil reports JSON null as (any); an absent key also means no probe.
    [ "$probe_type" != '(any)' ] || return 0
    [ "$probe_type" = dictionary ] && [ "$profile" = developer ] || return 1
    archive="$(/usr/bin/plutil -extract developer_probe.archive raw -expect string "$metadata")" || return 1
    schema="$(/usr/bin/plutil -extract developer_probe.schema_version raw -expect integer "$metadata")" || return 1
    digest="$(/usr/bin/plutil -extract developer_probe.sha256 raw -expect string "$metadata")" || return 1
    [ "$archive" = developer-probe-rootfs.tar ] && [ "$schema" = 1 ] || return 1
    [[ "$digest" =~ ^[0-9a-f]{64}$ ]] || return 1
    printf '%s' "$digest"
}

legacy_probe_file_matches() {
    local path="$1" expected="$2" measured
    [ -n "$expected" ] && [ -f "$path" ] && [ ! -L "$path" ] || return 1
    [ "$(/usr/bin/stat -f %l "$path")" = 1 ] || return 1
    measured="$(shasum -a 256 -- "$path")" || return 1
    [ "${measured%% *}" = "$expected" ]
}

copy_linux_profile_to_legacy_default() {
    local source_dir="$1" artifact probe_sha previous_sha
    local probe_name=developer-probe-rootfs.tar
    probe_sha="$(legacy_developer_probe_sha "$source_dir/version.json")" || {
        echo "error: invalid Developer probe metadata for legacy alias" >&2; return 1;
    }
    if [ -n "$probe_sha" ]; then
        legacy_probe_file_matches "$source_dir/$probe_name" "$probe_sha" || {
            echo "error: declared Developer probe is missing, redirected, or corrupt" >&2; return 1;
        }
    elif [ -e "$source_dir/$probe_name" ] || [ -L "$source_dir/$probe_name" ]; then
        echo "error: source bundle has an undeclared Developer probe" >&2; return 1
    fi
    # Capture old authority before replacing version.json. A basename alone is
    # not permission to remove or overwrite a foreign optional artifact.
    if [ -e "$LINUX_DIR/$probe_name" ] || [ -L "$LINUX_DIR/$probe_name" ]; then
        previous_sha="$(legacy_developer_probe_sha "$LINUX_DIR/version.json")" || return 1
        legacy_probe_file_matches "$LINUX_DIR/$probe_name" "$previous_sha" || {
            echo "error: existing legacy probe does not match its prior declaration; preserved" >&2; return 1;
        }
    fi
    mkdir -p "$LINUX_DIR"
    for artifact in vmlinux initramfs.img youki; do
        cp "$source_dir/$artifact" "$LINUX_DIR/$artifact"
    done
    if [ -n "$probe_sha" ]; then
        cp "$source_dir/$probe_name" "$LINUX_DIR/$probe_name"
    elif [ -e "$LINUX_DIR/$probe_name" ]; then
        rm -- "$LINUX_DIR/$probe_name"
    fi
    cp "$source_dir/version.json" "$LINUX_DIR/version.json"
    echo "  updated legacy default: $LINUX_DIR"
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

# --- Uninstall ---
#
# Removes exactly the software and runtime resources vz installed or created,
# named one at a time. Nothing is removed by wildcard and `$INSTALL_DIR` itself
# is never removed recursively: a user who keeps their own files under the
# prefix keeps them. Project directories, `vz.json` definitions, workspaces and
# the user's Docker configuration are never vz-owned and are never touched --
# every per-Machine Docker context vz creates lives inside the runtime data
# directory removed below, not in `~/.docker`.

VZ_BINARIES="vz vz-runtimed vz-guest-agent vz-agent-loader vz-macos-setup"
LINUX_ARTIFACTS="vmlinux initramfs.img youki version.json developer-probe-rootfs.tar"

removed_count=0
uninstall_dry_run=0

drop() {
    local path="$1"
    [ -e "$path" ] || [ -L "$path" ] || return 0
    if [ "$uninstall_dry_run" = 1 ]; then
        echo "  would remove: $path"
    else
        rm -rf -- "$path"
        echo "  removed: $path"
    fi
    removed_count=$((removed_count + 1))
}

drop_state_db() {
    local db="$1" suffix
    drop "$db"
    for suffix in -wal -shm -journal .startup-lock; do
        drop "${db}${suffix}"
    done
}

# The rc block `setup_path` appends, removed only when it is exactly that block.
remove_path_entry() {
    local shell_rc="$1"
    [ -f "$shell_rc" ] && [ ! -L "$shell_rc" ] || return 0
    grep -qF "export PATH=\"$BIN_DIR:\$PATH\"" "$shell_rc" || return 0
    if [ "$uninstall_dry_run" = 1 ]; then
        echo "  would remove the vz PATH entry from $shell_rc"
        return 0
    fi
    local trimmed
    trimmed="$(mktemp)"
    BIN_DIR="$BIN_DIR" /usr/bin/awk '
        $0 == "# vz" { pending = $0; next }
        $0 == "export PATH=\"" ENVIRON["BIN_DIR"] ":$PATH\"" { pending = ""; next }
        { if (pending != "") { print pending; pending = "" } print }
        END { if (pending != "") print pending }
    ' "$shell_rc" > "$trimmed"
    cat "$trimmed" > "$shell_rc"
    rm -f "$trimmed"
    echo "  removed the vz PATH entry from $shell_rc"
}

uninstall_main() {
    case "$INSTALL_DIR" in
        /) echo "error: refusing to uninstall from /" >&2; exit 1 ;;
        /*) ;;
        *) echo "error: VZ_INSTALL_DIR must be an absolute path: $INSTALL_DIR" >&2; exit 1 ;;
    esac
    if [ "$INSTALL_DIR" = "$HOME" ]; then
        echo "error: refusing to uninstall from the home directory itself" >&2
        exit 1
    fi

    # A live daemon owns running Machines, their disks and their sockets.
    # Removing its binaries and state underneath it would strand them, so this
    # refuses rather than terminating somebody else's processes.
    if command -v pgrep >/dev/null 2>&1 && pgrep -f "^$BIN_DIR/vz-runtimed" >/dev/null 2>&1; then
        echo "error: $BIN_DIR/vz-runtimed is running; stop your Environments first" >&2
        echo "       (vz stop, then retry the uninstall)" >&2
        exit 1
    fi

    local state_db runtime_dir binary artifact
    state_db="${VZ_RUNTIME_STATE_DB:-$INSTALL_DIR/stack-state.db}"
    runtime_dir="${VZ_RUNTIME_DATA_DIR:-$INSTALL_DIR/.vz-runtime}"

    echo "Uninstalling vz from $INSTALL_DIR..."
    for binary in $VZ_BINARIES; do
        drop "$BIN_DIR/$binary"
        drop "$BIN_DIR/$binary.sha256"
    done
    for artifact in $LINUX_ARTIFACTS; do
        drop "$LINUX_DIR/$artifact"
    done
    drop "$LINUX_DIR/developer"
    drop "$LINUX_DIR/container"
    drop "$INSTALL_DIR/machine-target-catalog.json"
    drop "$VERSION_FILE"

    echo "Removing vz-owned runtime resources..."
    drop_state_db "$state_db"
    drop "$runtime_dir"
    drop "$INSTALL_DIR/run"

    remove_path_entry "$HOME/.zshrc"
    remove_path_entry "$HOME/.bash_profile"

    if [ "$uninstall_dry_run" != 1 ]; then
        # Only when nothing else lives there. A non-empty directory is somebody
        # else's, and rmdir says so by failing.
        rmdir "$LINUX_DIR" 2>/dev/null || true
        rmdir "$BIN_DIR" 2>/dev/null || true
        rmdir "$INSTALL_DIR" 2>/dev/null || true
    fi

    echo ""
    echo "vz uninstalled ($removed_count vz-owned path(s))."
    echo "Project directories, vz.json definitions and workspaces were not touched."
    echo "Your Docker configuration was not touched: vz keeps its Machine contexts"
    echo "inside its own runtime directory."
}

# --- Main ---

print_getting_started() {
    echo "Get started with the installed version:"
    printf '  %q --help\n' "$BIN_DIR/vz"
    echo ""
    echo "The 0.4 Developer Environment design uses a project topology in vz.json."
    echo "Commands and capabilities depend on the installed version; consult its help."
    echo "Do not reuse the pre-0.4 sandbox configuration as a topology definition."
    echo "For native macOS without developer tools, prepare a local template:"
    printf '  %q\n' "$BIN_DIR/vz-macos-setup"
    echo "To also prepare an Xcode template, review its license and run:"
    printf '  %q --xcode /Applications/Xcode.app --accept-xcode-license\n' "$BIN_DIR/vz-macos-setup"
    echo "Setup requests administrator access once to provision the new guest disk."
    echo ""
    echo "To remove vz's software and runtime resources again (project directories,"
    echo "vz.json definitions, workspaces and your Docker configuration are kept):"
    echo "  sh install.sh --uninstall        (add --dry-run to list what it would remove)"
}

usage() {
    echo "Usage: install.sh [--uninstall [--dry-run]]"
    echo ""
    echo "  --uninstall   remove the vz software and runtime resources under"
    echo "                VZ_INSTALL_DIR (default \$HOME/.vz), leaving project"
    echo "                data and your Docker configuration alone"
    echo "  --dry-run     with --uninstall, list what would be removed"
    echo ""
    echo "Set VZ_LOCAL_RELEASE_DIR to install from a local release directory"
    echo "instead of downloading, verifying every file against its checksums.sha256."
}

main() {
    local action=install
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --uninstall) action=uninstall ;;
            --dry-run) uninstall_dry_run=1 ;;
            -h|--help) usage; exit 0 ;;
            *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
        esac
        shift
    done
    if [ "$action" = uninstall ]; then
        check_platform
        uninstall_main
        return
    fi
    if [ "$uninstall_dry_run" = 1 ]; then
        echo "error: --dry-run is only meaningful with --uninstall" >&2
        exit 2
    fi

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

    # A live daemon can own running Machines. Installation never kills it or
    # adopts its locks/sockets; incompatible clients fail with an explicit error.

    install_binaries "$version"
    install_linux_artifacts "$version"
    install_machine_catalog "$version"
    setup_path

    echo "$version" > "$VERSION_FILE"

    echo ""
    echo "vz v${version} installed successfully!"
    echo ""
    print_getting_started
}

main "$@"
