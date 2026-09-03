#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
VZ_BUILDKIT_REPO_ROOT=$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)
export VZ_BUILDKIT_REPO_ROOT
# shellcheck source=scripts/lib/buildkit-artifact-common.sh
. "$SCRIPT_DIR/lib/buildkit-artifact-common.sh"

usage() {
  echo "usage: $0 --output-dir DIR [--cache-dir DIR]" >&2
  exit 2
}

output_dir=
cache_dir=
started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-dir) [ "$#" -ge 2 ] || usage; output_dir=$2; shift 2 ;;
    --cache-dir) [ "$#" -ge 2 ] || usage; cache_dir=$2; shift 2 ;;
    *) usage ;;
  esac
done
[ -n "$output_dir" ] || usage

for tool in bsdtar curl git perl tar; do
  command -v "$tool" >/dev/null 2>&1 || vz_buildkit_die "$tool is required"
done
vz_buildkit_prepare_fresh_dir "$output_dir"

work_dir="$output_dir/.build-work"
mkdir -m 0700 "$work_dir"
partial=
cleanup() {
  if [ -n "$partial" ]; then
    rm -f "$partial"
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM
if [ -z "$cache_dir" ]; then
  cache_dir="$work_dir/download-cache"
fi
mkdir -p "$cache_dir"

version=$(vz_buildkit_contract_get buildkit_version)
source_repository=$(vz_buildkit_contract_get source_repository)
source_tag=$(vz_buildkit_contract_get source_tag)
source_commit=$(vz_buildkit_contract_get source_commit)
go_version=$(vz_buildkit_contract_get go_version)
archive_name=$(vz_buildkit_contract_get archive_name)
expected_archive_sha=$(vz_buildkit_contract_get archive_sha256)
contract_path=$(vz_buildkit_contract_path)
contract_sha=$(vz_buildkit_sha256 "$contract_path")

host_os=$(uname -s)
host_arch=$(uname -m)
case "$host_os/$host_arch" in
  Darwin/arm64) go_platform=darwin-arm64; go_sha_key=go_darwin_arm64_sha256 ;;
  Linux/x86_64|Linux/amd64) go_platform=linux-amd64; go_sha_key=go_linux_amd64_sha256 ;;
  Linux/aarch64|Linux/arm64) go_platform=linux-arm64; go_sha_key=go_linux_arm64_sha256 ;;
  *) vz_buildkit_die "unsupported builder host: $host_os/$host_arch" ;;
esac
go_archive="go${go_version}.${go_platform}.tar.gz"
go_archive_path="$cache_dir/$go_archive"
go_archive_sha=$(vz_buildkit_contract_get "$go_sha_key")
if [ -f "$go_archive_path" ]; then
  [ "$(vz_buildkit_sha256 "$go_archive_path")" = "$go_archive_sha" ] || \
    vz_buildkit_die "cached Go archive checksum mismatch: $go_archive_path"
else
  partial="$go_archive_path.partial.$$"
  curl --fail --location --proto '=https' --tlsv1.2 \
    --output "$partial" "https://go.dev/dl/$go_archive" >"$output_dir/toolchain-download.log" 2>&1
  [ "$(vz_buildkit_sha256 "$partial")" = "$go_archive_sha" ] || vz_buildkit_die "downloaded Go archive checksum mismatch"
  mv "$partial" "$go_archive_path"
  partial=
fi

tar -xzf "$go_archive_path" -C "$work_dir"
go_bin="$work_dir/go/bin/go"
[ -x "$go_bin" ] || vz_buildkit_die "pinned Go toolchain is incomplete"
[ "$($go_bin version)" = "go version go${go_version} ${go_platform%-*}/${go_platform#*-}" ] || \
  vz_buildkit_die "pinned Go toolchain reports an unexpected version"

source_dir="$work_dir/buildkit"
git clone --branch "$source_tag" --depth 1 "$source_repository" "$source_dir" >"$output_dir/source-fetch.log" 2>&1
actual_commit=$(git -C "$source_dir" rev-parse HEAD)
[ "$actual_commit" = "$source_commit" ] || vz_buildkit_die "BuildKit source commit mismatch: expected $source_commit, found $actual_commit"
[ -z "$(git -C "$source_dir" status --porcelain)" ] || vz_buildkit_die "BuildKit source checkout is not clean"
source_tree=$(git -C "$source_dir" rev-parse 'HEAD^{tree}')

mkdir -p "$work_dir/cache" "$work_dir/gopath" "$work_dir/out"
package_path=github.com/moby/buildkit
ldflags="-X ${package_path}/version.Version=v${version} -X ${package_path}/version.Revision=${source_commit} -X ${package_path}/version.Package=${package_path}"
(
  cd "$source_dir"
  env GOENV=off GOTOOLCHAIN=local GOCACHE="$work_dir/cache" GOPATH="$work_dir/gopath" \
    GOOS=linux GOARCH=arm64 GOARM64=v8.0 CGO_ENABLED=0 GOFLAGS=-mod=vendor \
    "$go_bin" build -trimpath -ldflags "$ldflags" -o "$work_dir/out/buildctl" ./cmd/buildctl
) >"$output_dir/buildctl-build.log" 2>&1
(
  cd "$source_dir"
  env GOENV=off GOTOOLCHAIN=local GOCACHE="$work_dir/cache" GOPATH="$work_dir/gopath" \
    GOOS=linux GOARCH=arm64 GOARM64=v8.0 CGO_ENABLED=0 GOFLAGS=-mod=vendor \
    "$go_bin" build -trimpath -ldflags "$ldflags -extldflags '-static'" \
    -tags 'osusergo netgo static_build seccomp' -o "$work_dir/out/buildkitd" ./cmd/buildkitd
) >"$output_dir/buildkitd-build.log" 2>&1

for binary in buildctl buildkitd; do
  expected_binary_sha=$(vz_buildkit_contract_get "${binary}_sha256")
  actual_binary_sha=$(vz_buildkit_sha256 "$work_dir/out/$binary")
  [ "$actual_binary_sha" = "$expected_binary_sha" ] || \
    vz_buildkit_die "$binary reproducibility mismatch: expected $expected_binary_sha, found $actual_binary_sha"
done

package_dir="$work_dir/package"
mkdir -p "$package_dir/bin"
install -m 0755 "$work_dir/out/buildctl" "$package_dir/bin/buildctl"
install -m 0755 "$work_dir/out/buildkitd" "$package_dir/bin/buildkitd"
vz_buildkit_write_manifest "$package_dir/manifest.json"
chmod 0644 "$package_dir/manifest.json"
TZ=UTC touch -t 197001010000 "$package_dir/manifest.json" "$package_dir/bin/buildctl" "$package_dir/bin/buildkitd"
archive_path="$output_dir/$archive_name"
LC_ALL=C TZ=UTC bsdtar -cf "$archive_path" \
  --format=ustar --uid 0 --gid 0 --uname root --gname root \
  --no-xattrs --no-acls --no-fflags -C "$package_dir" \
  manifest.json bin/buildctl bin/buildkitd
actual_archive_sha=$(vz_buildkit_sha256 "$archive_path")
[ "$actual_archive_sha" = "$expected_archive_sha" ] || \
  vz_buildkit_die "archive reproducibility mismatch: expected $expected_archive_sha, found $actual_archive_sha"
printf '%s  %s\n' "$actual_archive_sha" "$archive_name" > "$archive_path.sha256"

buildctl_size=$(wc -c < "$work_dir/out/buildctl" | tr -d ' ')
buildkitd_size=$(wc -c < "$work_dir/out/buildkitd" | tr -d ' ')
manifest_sha=$(vz_buildkit_sha256 "$package_dir/manifest.json")
manifest_size=$(wc -c < "$package_dir/manifest.json" | tr -d ' ')
archive_size=$(wc -c < "$archive_path" | tr -d ' ')
builder_sha=$(vz_buildkit_sha256 "$SCRIPT_DIR/build-runtime-free-buildkit.sh")
validator_sha=$(vz_buildkit_sha256 "$SCRIPT_DIR/validate-runtime-free-buildkit.sh")
printf '%s\n' "$($go_bin version)" > "$output_dir/toolchain.txt"
printf 'repository=%s\ntag=%s\ncommit=%s\ntree=%s\nclean=true\n' \
  "$source_repository" "$source_tag" "$source_commit" "$source_tree" > "$output_dir/source.txt"
buildctl_log_sha=$(vz_buildkit_sha256 "$output_dir/buildctl-build.log")
buildkitd_log_sha=$(vz_buildkit_sha256 "$output_dir/buildkitd-build.log")
source_fetch_log_sha=$(vz_buildkit_sha256 "$output_dir/source-fetch.log")
toolchain_file_sha=$(vz_buildkit_sha256 "$output_dir/toolchain.txt")
source_file_sha=$(vz_buildkit_sha256 "$output_dir/source.txt")
git_version=$(vz_buildkit_json_escape "$(git --version)")
bsdtar_version=$(vz_buildkit_json_escape "$(bsdtar --version | sed -n '1p')")
curl_version=$(vz_buildkit_json_escape "$(curl --version | sed -n '1p')")
tar_version=$(vz_buildkit_json_escape "$(tar --version 2>&1 | sed -n '1p')")
perl_version=$(vz_buildkit_json_escape "$(perl -e 'printf "%vd", $^V')")
if command -v sha256sum >/dev/null 2>&1; then
  hash_tool=$(vz_buildkit_json_escape "$(sha256sum --version | sed -n '1p')")
else
  hash_tool=$(vz_buildkit_json_escape "$(shasum --version 2>&1 | sed -n '1p')")
fi
completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
cat > "$output_dir/buildkit-artifact-provenance.json" <<EOF
{
  "schema_version": 1,
  "source_mode": "candidate-build",
  "started_at_utc": "$started_at",
  "completed_at_utc": "$completed_at",
  "host_os": "$host_os",
  "host_arch": "$host_arch",
  "source_repository": "$source_repository",
  "source_tag": "$source_tag",
  "source_commit": "$source_commit",
  "source_tree": "$source_tree",
  "source_head_matches_pin": true,
  "source_checkout_clean": true,
  "go_version": "$go_version",
  "go_url": "https://go.dev/dl/$go_archive",
  "go_archive": "$go_archive",
  "go_archive_sha256": "$go_archive_sha",
  "contract_path": "config/buildkit-artifact-v0.19.0.json",
  "contract_sha256": "$contract_sha",
  "builder_sha256": "$builder_sha",
  "validator_sha256": "$validator_sha",
  "tools": {
    "git": "$git_version",
    "bsdtar": "$bsdtar_version",
    "curl": "$curl_version",
    "tar": "$tar_version",
    "perl": "$perl_version",
    "sha256": "$hash_tool"
  },
  "build": {
    "direct_go_invocation_count": 2,
    "environment": ["GOENV=off", "GOTOOLCHAIN=local", "GOOS=linux", "GOARCH=arm64", "GOARM64=v8.0", "CGO_ENABLED=0", "GOFLAGS=-mod=vendor"],
    "common_flags": ["-trimpath"],
    "ldflags": "$ldflags",
    "buildkitd_ldflags_suffix": "-extldflags '-static'",
    "buildkitd_tags": ["osusergo", "netgo", "static_build", "seccomp"]
  },
  "raw_evidence": [
    {"path": "buildctl-build.log", "sha256": "$buildctl_log_sha"},
    {"path": "buildkitd-build.log", "sha256": "$buildkitd_log_sha"},
    {"path": "source-fetch.log", "sha256": "$source_fetch_log_sha"},
    {"path": "toolchain.txt", "sha256": "$toolchain_file_sha"},
    {"path": "source.txt", "sha256": "$source_file_sha"}
  ],
  "entries": [
    {"path": "manifest.json", "mode": "0644", "size": $manifest_size, "sha256": "$manifest_sha"},
    {"path": "bin/buildctl", "mode": "0755", "size": $buildctl_size, "sha256": "$(vz_buildkit_contract_get buildctl_sha256)"},
    {"path": "bin/buildkitd", "mode": "0755", "size": $buildkitd_size, "sha256": "$(vz_buildkit_contract_get buildkitd_sha256)"}
  ],
  "archive": {"name": "$archive_name", "format": "ustar", "size": $archive_size, "sha256": "$actual_archive_sha", "inventory": ["manifest.json", "bin/buildctl", "bin/buildkitd"]},
  "fallbacks": [],
  "retries": [],
  "verdict": "passed"
}
EOF

validation_dir="$work_dir/validation"
"$SCRIPT_DIR/validate-runtime-free-buildkit.sh" \
  --archive "$archive_path" \
  --expected-sha256 "$expected_archive_sha" \
  --output-dir "$validation_dir" \
  --source-mode candidate-build
for evidence in manifest.json buildkit-artifact-inventory.txt buildkit-artifact-verification.json; do
  cp "$validation_dir/$evidence" "$output_dir/$evidence"
done
chmod 0644 "$output_dir"/*
evidence_manifest="$output_dir/buildkit-artifact-evidence.sha256"
for evidence in \
  "$archive_name" "$archive_name.sha256" \
  manifest.json buildkit-artifact-inventory.txt \
  buildkit-artifact-provenance.json buildkit-artifact-verification.json \
  buildctl-build.log buildkitd-build.log source-fetch.log source.txt toolchain.txt; do
  printf '%s  %s\n' "$(vz_buildkit_sha256 "$output_dir/$evidence")" "$evidence" >> "$evidence_manifest"
done
if [ -f "$output_dir/toolchain-download.log" ]; then
  printf '%s  %s\n' "$(vz_buildkit_sha256 "$output_dir/toolchain-download.log")" toolchain-download.log >> "$evidence_manifest"
fi
while read -r expected_evidence_sha evidence; do
  [ "$(vz_buildkit_sha256 "$output_dir/$evidence")" = "$expected_evidence_sha" ] || \
    vz_buildkit_die "evidence checksum mismatch: $evidence"
done < "$evidence_manifest"
chmod 0644 "$evidence_manifest"
rm -rf "$work_dir"
trap - EXIT HUP INT TERM
echo "built canonical runtime-free BuildKit archive: $archive_path"
