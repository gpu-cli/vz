#!/usr/bin/env bash
# Source and compiler workspaces live in an exactly-labelled Linux Docker volume,
# never in the (possibly case-insensitive) host checkout mount.
set -euo pipefail
[[ $# == 5 ]] || { echo 'usage: docker-build.sh REPO_ROOT OUT_DIR CACHE_DIR PROFILE JOBS' >&2; exit 2; }
repo_root="$1"
out_dir="$2"
cache_dir="$3"
profile="$4"
jobs="$5"
build_target="${LINUX_DOCKER_TARGET:-all}"
[[ "$repo_root" == /* && "$out_dir" == /* && "$cache_dir" == /* ]] || { echo 'build paths must be absolute' >&2; exit 2; }
[[ "$out_dir" != / && "$out_dir" != "$repo_root" && "$cache_dir" != / && "$cache_dir" != "$repo_root" ]] || { echo 'output/cache must be dedicated directories' >&2; exit 2; }
[[ "$profile" == developer || "$profile" == container ]] || { echo 'invalid kernel profile' >&2; exit 2; }
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo 'positive build parallelism required' >&2; exit 2; }
[[ "$build_target" == all || "$build_target" == kernel || "$build_target" == source-check ]] || { echo 'unsupported Linux build target' >&2; exit 2; }
if [[ "$build_target" == all && -n "${YOUKI_CACHE_DIR:-}" ]]; then
    echo 'docker-build all uses the checkout youki cache; unset YOUKI_CACHE_DIR (external cache overrides are not mounted)' >&2
    exit 2
fi
docker_context="${LINUX_DOCKER_CONTEXT:-}"
[[ -n "$docker_context" ]] || { echo 'set LINUX_DOCKER_CONTEXT explicitly (for example orbstack)' >&2; exit 2; }
docker_command=(env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH -u BUILDX_BUILDER -u BUILDX_CONFIG docker --context "$docker_context")
endpoint="$("${docker_command[@]}" context inspect "$docker_context" --format '{{.Endpoints.docker.Host}}')"
[[ "$endpoint" == unix:///* ]] || { echo 'kernel builds require an explicitly selected local Unix-socket Docker context' >&2; exit 2; }
builder="${LINUX_DOCKER_BUILDER:-vz-linux-builder}"
if [[ -z "${LINUX_DOCKER_BUILDER:-}" ]]; then
    "${docker_command[@]}" build --platform linux/arm64 -t "$builder" "$repo_root/linux"
fi
identity="$("${docker_command[@]}" image inspect "$builder" --format '{{.Id}} {{.Os}} {{.Architecture}}')"
read -r builder_id builder_os builder_arch <<< "$identity"
[[ "$builder_id" =~ ^sha256:[0-9a-f]{64}$ && "$builder_os" == linux && "$builder_arch" == arm64 ]] || { echo 'builder must be a local Linux/arm64 image' >&2; exit 2; }
expected_rust="$(sed -n 's/^ARG VZ_LINUX_RUST_VERSION=//p' "$repo_root/linux/Dockerfile")"
[[ "$expected_rust" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'missing pinned builder Rust version' >&2; exit 2; }
actual_rust="$("${docker_command[@]}" run --rm --network none "$builder_id" rustc --version)"
[[ "$actual_rust" == "rustc $expected_rust ("* ]] || { echo "builder Rust mismatch: expected $expected_rust, found $actual_rust" >&2; exit 2; }
fragment=vz-linux.config
if [[ "$profile" == container ]]; then fragment=vz-linux-container.config; fi
recipe_digest="$(
    {
        cd "$repo_root/linux"
        shasum -a 256 Makefile Dockerfile kernel-version.mk source-build.py docker-build.sh "$fragment"
        echo "$builder_id $profile"
    } | shasum -a 256 | cut -d' ' -f1
)"
volume="vz-linux-source-v1-$profile-${recipe_digest:0:32}"
if ! "${docker_command[@]}" volume inspect "$volume" >/dev/null 2>&1; then
    "${docker_command[@]}" volume create \
        --label io.vz.owner=linux-source-builder-v1 \
        --label "io.vz.profile=$profile" --label "io.vz.inputs=$recipe_digest" "$volume" >/dev/null
fi
labels="$("${docker_command[@]}" volume inspect "$volume" --format '{{index .Labels "io.vz.owner"}} {{index .Labels "io.vz.profile"}} {{index .Labels "io.vz.inputs"}}')"
[[ "$labels" == "linux-source-builder-v1 $profile $recipe_digest" ]] || { echo "refusing foreign/mismatched build volume: $volume" >&2; exit 1; }
[[ ! -L "$out_dir" && ! -L "$cache_dir" ]] || { echo 'output/cache directories must not be symlinks' >&2; exit 2; }
mkdir -p "$out_dir" "$cache_dir"
echo "Builder=$builder_id profile=$profile source_volume=$volume target=$build_target jobs=$jobs"
"${docker_command[@]}" run --rm --ulimit nofile=1048576:1048576 \
    -v "$repo_root:/workspace:ro" -v "$out_dir:/vz-output" -v "$cache_dir:/vz-cache" \
    --mount "type=volume,source=$volume,target=/vz-build" \
    -e "VZ_LINUX_BUILDER_ID=$builder_id" -e CARGO_TARGET_DIR=/vz-build/cargo-target \
    -e CARGO_HOME=/vz-build/cargo-home -e "CARGO_BUILD_JOBS=$jobs" -e RUSTC_WRAPPER= \
    -w /workspace/linux "$builder_id" bash -ceu '
        mkdir -p /vz-build/src /vz-build/work /vz-build/cargo-home
        chmod 700 /vz-build /vz-build/src /vz-build/work /vz-build/cargo-home
        exec flock /vz-build/.profile-build.lock make "$3" \
            KERNEL_PROFILE="$1" JOBS="$2" CROSS_COMPILE= \
            OUT_DIR=/vz-output CACHE_DIR=/vz-cache SRC_DIR=/vz-build/src BUILD_DIR=/vz-build/work \
            GUEST_AGENT_BUILD_TOOL=cargo \
            GUEST_AGENT_BINARY=/vz-build/cargo-target/aarch64-unknown-linux-musl/release/vz-guest-agent
    ' -- "$profile" "$jobs" "$build_target"
