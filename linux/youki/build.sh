#!/usr/bin/env bash
# Build one immutable, feature-complete ARM64-musl runtime for both Linux profiles.
set -euo pipefail
recipe_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$recipe_dir/inputs.env"
mode="${1:---fetch-only}"
if [[ "$mode" != --fetch-only && "$mode" != --build-only && "$mode" != --install ]]; then
    echo 'usage: build.sh --fetch-only | --build-only | --install /absolute/output/youki' >&2
    exit 2
fi
docker_context="${YOUKI_DOCKER_CONTEXT:-}"
docker_command=(env -u BUILDX_BUILDER -u BUILDX_CONFIG -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH docker --context "$docker_context")
cache_root="${YOUKI_CACHE_DIR:-$recipe_dir/../.cache/youki-source}"
mkdir -p "$cache_root/downloads" "$cache_root/builds"
recipe_digest="$(cd "$recipe_dir" && shasum -a 256 Dockerfile inputs.env apk.sha256 build.sh validate.py lock.py seccomp-exec.patch tenant-root.patch runtime-log.patch executable-permissions.patch tenant-cgroup.patch run-keep.patch | shasum -a 256 | cut -d' ' -f1)"
candidate="$cache_root/builds/$recipe_digest"

fetch() {
    local url="$1" digest="$2" destination="$3" partial
    if [[ -e "$destination" || -L "$destination" ]]; then
        [[ -f "$destination" && ! -L "$destination" ]] || { echo "unsafe cached input: $destination" >&2; exit 1; }
        [[ "$(shasum -a 256 "$destination" | cut -d' ' -f1)" == "$digest" ]] || { echo "cached input checksum mismatch: $destination" >&2; exit 1; }
        return
    fi
    partial="$(mktemp "$cache_root/downloads/.fetch.XXXXXX")"
    curl --fail --location --proto '=https' --tlsv1.2 "$url" -o "$partial"
    [[ "$(shasum -a 256 "$partial" | cut -d' ' -f1)" == "$digest" ]] || { echo "download checksum mismatch: $url (retained $partial)" >&2; exit 1; }
    mv "$partial" "$destination"
}

if [[ ! -d "$candidate" ]]; then
    [[ -n "$docker_context" ]] || { echo 'set YOUKI_DOCKER_CONTEXT explicitly for Docker builds or pulls (for example orbstack on macOS)' >&2; exit 2; }
    if [[ -z "${VZ_YOUKI_BUILD_LOCK_FD:-}" ]]; then
        exec python3 "$recipe_dir/lock.py" "$cache_root/advisory.lock" bash "$recipe_dir/build.sh" "$@"
    fi
    docker_endpoint="$("${docker_command[@]}" context inspect "$docker_context" --format '{{.Endpoints.docker.Host}}')"
    [[ "$docker_endpoint" == unix:///* ]] || { echo 'youki builds require an explicitly selected local Unix-socket Docker context' >&2; exit 2; }
    "${docker_command[@]}" buildx inspect "$docker_context" | python3 -c '
import sys
lines = sys.stdin.read().splitlines()
drivers = [line.split(":", 1)[1].strip() for line in lines if line.startswith("Driver:")]
endpoints = [line.split(":", 1)[1].strip() for line in lines if line.startswith("Endpoint:")]
if drivers != ["docker"] or endpoints != [sys.argv[1]]:
    raise SystemExit("refusing Buildx builder not bound to the selected local Docker context")
' "$docker_context"
    fetch "https://codeload.github.com/youki-dev/youki/tar.gz/$YOUKI_COMMIT" "$YOUKI_SOURCE_SHA256" "$cache_root/downloads/source-$YOUKI_COMMIT.tar.gz"
    while read -r digest filename; do
        fetch "https://dl-cdn.alpinelinux.org/alpine/v3.22/main/aarch64/$filename" "$digest" "$cache_root/downloads/$filename"
    done < "$recipe_dir/apk.sha256"
    if [[ "$mode" == --fetch-only ]]; then
        "${docker_command[@]}" pull --platform linux/arm64 "$YOUKI_BUILDER"
        exit 0
    fi
    context="$(mktemp -d "$cache_root/builds/.context.XXXXXX")"
    mkdir "$context/apks"
    cp "$recipe_dir/Dockerfile" "$recipe_dir/inputs.env" "$recipe_dir/apk.sha256" "$context/"
    cp "$recipe_dir/seccomp-exec.patch" "$context/"
    cp "$recipe_dir/tenant-root.patch" "$context/"
    cp "$recipe_dir/runtime-log.patch" "$context/"
    cp "$recipe_dir/executable-permissions.patch" "$context/"
    cp "$recipe_dir/tenant-cgroup.patch" "$context/"
    cp "$recipe_dir/run-keep.patch" "$context/"
    cp "$cache_root/downloads/source-$YOUKI_COMMIT.tar.gz" "$context/source.tar.gz"
    while read -r _ filename; do cp "$cache_root/downloads/$filename" "$context/apks/"; done < "$recipe_dir/apk.sha256"
    output="$(mktemp -d "$cache_root/builds/.candidate.XXXXXX")"
    # Bounded jobs and a dedicated image avoid competing kernel/guest toolchains.
    "${docker_command[@]}" buildx build --builder "$docker_context" --platform linux/arm64 \
        --build-arg "YOUKI_BUILDER=$YOUKI_BUILDER" --target artifact \
        --output "type=local,dest=$output" --progress plain "$context" 2>&1 | tee "$context/build.log"
    python3 "$recipe_dir/validate.py" "$output" "$recipe_dir"
    mv "$output" "$candidate"
    # Retain exact build context and downloads for reproducibility/debugging.
fi

digest="$(python3 "$recipe_dir/validate.py" "$candidate" "$recipe_dir")"
if [[ "$mode" != --install ]]; then
    echo "Verified candidate: $candidate (sha256=$digest)"
    exit 0
fi
destination="${2:?--install requires an absolute output path}"
[[ "$destination" == /*/youki ]] || { echo 'output must be an absolute youki file path' >&2; exit 2; }
mkdir -p "$(dirname "$destination")"
if [[ -e "$destination" || -L "$destination" ]]; then
    [[ -f "$destination" && ! -L "$destination" ]] || { echo "refusing non-regular output: $destination" >&2; exit 1; }
    python3 -c 'import os,sys; s=os.lstat(sys.argv[1]); sys.exit(0 if s.st_nlink == 1 else "refusing hardlinked youki output")' "$destination"
    if [[ "$(shasum -a 256 "$destination" | cut -d' ' -f1)" == "$digest" ]] && \
        python3 -c 'import os,stat,sys; sys.exit(0 if stat.S_IMODE(os.lstat(sys.argv[1]).st_mode) == 0o755 else 1)' "$destination"; then exit 0; fi
fi
staged="$(mktemp "$(dirname "$destination")/.youki.XXXXXX")"
cp "$candidate/youki" "$staged"
chmod 755 "$staged"
[[ "$(shasum -a 256 "$staged" | cut -d' ' -f1)" == "$digest" ]]
mv -f "$staged" "$destination"
echo "Installed verified youki $YOUKI_VERSION features=$YOUKI_FEATURES sha256=$digest"
