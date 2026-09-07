#!/bin/bash
# Guest bundle helpers for the vz 0.4 release-candidate builder.
#
# The rebuild loop is copied verbatim in spirit from
# scripts/run-sandbox-vm-e2e.sh:699-723 (sourcing that script would run its
# entire E2E preamble). The staging/verification helpers implement the same
# checks as installed_developer_startup.bundle_inputs (:111-129): every declared
# digest in version.json must match the artifact bytes, the Developer profile
# must carry the schema-1 startup probe archive, and the Hardened (container)
# profile must not acquire one.
#
# Callers must `set -euo pipefail` and may define `err` (message -> exit 1).

if ! declare -F err >/dev/null; then
    err() { printf 'error: %s\n' "$*" >&2; exit 1; }
fi

VZ04_GUEST_PROFILES=(developer container)

# vz04_guest_bundle_source_dir <repo_root> <profile>
# Where `make -C linux ... docker-build` leaves each profile's bundle.
vz04_guest_bundle_source_dir() {
    case "$2" in
        developer) printf '%s/linux/out\n' "$1" ;;
        container) printf '%s/linux/out/container\n' "$1" ;;
        *) err "unknown guest profile: $2" ;;
    esac
}

# vz04_rebuild_guest_bundles <repo_root> <log_dir>
# Rebuild both Linux guest bundles from this checkout through the Docker builder.
vz04_rebuild_guest_bundles() {
    local repo_root="$1" log_dir="$2" kernel_profile
    # The VM executes the Linux guest agent embedded in each profile's initramfs,
    # not the macOS host binary. Rebuild both bundles on every run so source
    # changes cannot be silently shipped with a stale guest executable.
    local guest_agent_build_tool="${VZ_E2E_GUEST_AGENT_BUILD_TOOL:-cargo}"
    if [[ "$guest_agent_build_tool" != "cargo" ]]; then
        err "guest bundles now build on case-sensitive Linux storage; VZ_E2E_GUEST_AGENT_BUILD_TOOL must be cargo"
    fi
    if [[ -z "${LINUX_DOCKER_CONTEXT:-}" ]]; then
        err "set LINUX_DOCKER_CONTEXT to an explicit local Unix-socket Docker build context"
    fi
    for kernel_profile in "${VZ04_GUEST_PROFILES[@]}"; do
        echo "==> rebuilding Linux $kernel_profile guest bundle"
        # The wrapper verifies source/config/compiler provenance before reusing a
        # kernel and rebuilds guest userland from this checkout. A direct Mac make
        # would extract case-distinct BusyBox/kernel inputs on host storage and
        # cannot verify the native Linux compiler identity of cached artifacts.
        make -C "$repo_root/linux" \
            KERNEL_PROFILE="$kernel_profile" \
            LINUX_DOCKER_TARGET=all \
            docker-build 2>&1 | tee "$log_dir/linux-$kernel_profile-build.log"
    done
}

# vz04_regular_file <path>
# A bundle input must be a regular, single-link, non-symlink file.
vz04_regular_file() {
    [[ -f "$1" && ! -L "$1" ]] || err "bundle input must be a regular non-symlink file: $1"
    [[ "$(/usr/bin/stat -f '%l' "$1")" == 1 ]] || err "bundle input must have a single hard link: $1"
}

vz04_sha256() {
    shasum -a 256 "$1" | cut -d' ' -f1
}

# vz04_guest_bundle_files <profile>
# The exact file set a staged bundle contains.
vz04_guest_bundle_files() {
    case "$1" in
        developer) printf '%s\n' vmlinux initramfs.img youki version.json developer-probe-rootfs.tar ;;
        container) printf '%s\n' vmlinux initramfs.img youki version.json ;;
        *) err "unknown guest profile: $1" ;;
    esac
}

# vz04_verify_guest_bundle <dir> <profile>
# Fail unless the directory holds a complete, self-consistent bundle for <profile>.
vz04_verify_guest_bundle() {
    local dir="$1" profile="$2" version name key declared actual probe_sha
    version="$dir/version.json"
    vz04_regular_file "$version"
    [[ "$(/usr/bin/stat -f '%z' "$version")" -le $((4 * 1024 * 1024)) ]] || err "bounded version.json required: $version"
    [[ "$(jq -r '.profile' "$version")" == "$profile" ]] || err "wrong bundle profile in $version"
    for name in vmlinux initramfs.img youki; do
        case "$name" in
            vmlinux) key=sha256_vmlinux ;;
            initramfs.img) key=sha256_initramfs ;;
            youki) key=sha256_youki ;;
        esac
        vz04_regular_file "$dir/$name"
        declared="$(jq -r --arg key "$key" '.[$key] // empty' "$version")"
        [[ "$declared" =~ ^[0-9a-f]{64}$ ]] || err "version.json lacks a canonical $key"
        actual="$(vz04_sha256 "$dir/$name")"
        [[ "$actual" == "$declared" ]] || err "declared artifact digest differs: $dir/$name"
    done
    if [[ "$profile" == developer ]]; then
        [[ "$(jq -r '.developer_probe | type' "$version")" == object ]] || err "new normal Developer probe bundle required"
        [[ "$(jq -r '.developer_probe.schema_version' "$version")" == 1 ]] || err "Developer probe schema_version 1 required"
        [[ "$(jq -r '.developer_probe.archive' "$version")" == developer-probe-rootfs.tar ]] \
            || err "Developer probe archive must be developer-probe-rootfs.tar"
        vz04_regular_file "$dir/developer-probe-rootfs.tar"
        probe_sha="$(jq -r '.developer_probe.sha256' "$version")"
        [[ "$(vz04_sha256 "$dir/developer-probe-rootfs.tar")" == "$probe_sha" ]] || err "startup archive digest mismatch: $dir"
    else
        [[ "$(jq -r '.developer_probe' "$version")" == null ]] || err "Hardened must not acquire Docker probe (version.json)"
        [[ ! -e "$dir/developer-probe-rootfs.tar" ]] || err "Hardened must not acquire Docker probe (archive present)"
    fi
}

# vz04_stage_guest_bundle <source_dir> <profile> <dest_dir>
# Copy the exact bundle file set read-only into <dest_dir>, verifying bytes did
# not change in transit. <dest_dir> must not exist.
vz04_stage_guest_bundle() {
    local source_dir="$1" profile="$2" dest_dir="$3" name
    vz04_verify_guest_bundle "$source_dir" "$profile"
    [[ ! -e "$dest_dir" ]] || err "bundle destination already exists: $dest_dir"
    mkdir -m 0755 "$dest_dir"
    while IFS= read -r name; do
        cp "$source_dir/$name" "$dest_dir/$name"
        if [[ "$name" == youki ]]; then
            chmod 0555 "$dest_dir/$name"
        else
            chmod 0444 "$dest_dir/$name"
        fi
        [[ "$(vz04_sha256 "$dest_dir/$name")" == "$(vz04_sha256 "$source_dir/$name")" ]] \
            || err "bundle staging changed bytes: $dest_dir/$name"
    done < <(vz04_guest_bundle_files "$profile")
    vz04_verify_guest_bundle "$dest_dir" "$profile"
}

# vz04_dir_content_sha256 <dir> <vz04_source_tree.py>
# sha256(canonical_json(sorted [[relpath, sha256]...]) + LF) over every regular
# file below <dir>; symlinks and other non-regular entries are rejected.
vz04_dir_content_sha256() {
    local dir="$1" helper="$2" relpath
    [[ -f "$helper" ]] || err "source-tree helper missing: $helper"
    (
        cd "$dir" || exit 1
        while IFS= read -r -d '' relpath; do
            relpath="${relpath#./}"
            [[ -f "$relpath" && ! -L "$relpath" ]] || err "non-regular entry in bundle directory: $dir/$relpath"
            jq -cn --arg path "$relpath" --arg sha "$(vz04_sha256 "$relpath")" '[$path, $sha]'
        done < <(find . -mindepth 1 ! -type d -print0 | LC_ALL=C sort -z)
    ) | jq -sc 'sort' | /usr/bin/python3 "$helper" canonical-sha256
}
