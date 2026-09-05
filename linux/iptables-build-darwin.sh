#!/usr/bin/env bash
# iptables has case-distinct sources and headers: never extract it on the Mac
# checkout filesystem. Build in the local Linux builder's private /tmp instead.
set -euo pipefail
[[ $# == 6 ]] || { echo 'usage: iptables-build-darwin.sh REPO_ROOT OUT_DIR ARCHIVE VERSION SHA256 JOBS' >&2; exit 2; }
repo_root="$1"
out_dir="$2"
archive="$3"
version="$4"
expected_sha="$5"
jobs="$6"
[[ "$repo_root" == /* && "$out_dir" == /* && "$archive" == /* ]] || { echo 'iptables build paths must be absolute' >&2; exit 2; }
[[ "$jobs" =~ ^[1-9][0-9]*$ && "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "$expected_sha" =~ ^[0-9a-f]{64}$ ]] || { echo 'invalid iptables build input' >&2; exit 2; }
[[ -f "$archive" && ! -L "$archive" ]] || { echo 'iptables archive must be a regular non-symlink file' >&2; exit 2; }
[[ "$(shasum -a 256 "$archive" | cut -d' ' -f1)" == "$expected_sha" ]] || { echo 'iptables source archive checksum mismatch' >&2; exit 1; }
docker_context="${IPTABLES_DOCKER_CONTEXT:-}"
[[ -n "$docker_context" ]] || { echo 'set IPTABLES_DOCKER_CONTEXT explicitly (for example orbstack); prepare vz-linux-builder using linux/Dockerfile' >&2; exit 2; }
docker_command=(env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH -u BUILDX_BUILDER -u BUILDX_CONFIG docker --context "$docker_context")
endpoint="$("${docker_command[@]}" context inspect "$docker_context" --format '{{.Endpoints.docker.Host}}')"
[[ "$endpoint" == unix:///* ]] || { echo 'iptables builds require an explicitly selected local Unix-socket Docker context' >&2; exit 2; }
builder="${IPTABLES_DOCKER_BUILDER:-vz-linux-builder}"
identity="$("${docker_command[@]}" image inspect "$builder" --format '{{.Id}} {{.Os}} {{.Architecture}}')" || {
    echo 'prepare the local builder: docker --context <context> build -t vz-linux-builder linux' >&2; exit 1;
}
read -r builder_id builder_os builder_arch <<< "$identity"
[[ "$builder_id" =~ ^sha256:[0-9a-f]{64}$ && "$builder_os" == linux && "$builder_arch" == arm64 ]] || { echo 'iptables builder must be a local Linux/arm64 image' >&2; exit 2; }
mkdir -p "$out_dir"
echo "Building pinned iptables $version with local builder $builder_id; case-sensitive source in container-private /tmp"
"${docker_command[@]}" run --rm --network none --ulimit nofile=1048576:1048576 \
    -v "$repo_root:/workspace:ro" -v "$out_dir:/vz-iptables-output" \
    -v "$archive:/vz-iptables-source.tar.xz:ro" -w /workspace/linux "$builder_id" \
    flock /vz-iptables-output/.iptables-build.lock \
    make iptables KERNEL_PROFILE=developer OUT_DIR=/vz-iptables-output \
        SRC_DIR=/tmp/vz-iptables-src CACHE_DIR=/tmp/vz-iptables-cache \
        IPTABLES_SRC_DIR=/tmp/vz-iptables-src IPTABLES_ARCHIVE=/vz-iptables-source.tar.xz \
        IPTABLES_VERSION="$version" IPTABLES_ARCHIVE_SHA256="$expected_sha" JOBS="$jobs"
