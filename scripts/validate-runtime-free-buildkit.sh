#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
VZ_BUILDKIT_REPO_ROOT=$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)
export VZ_BUILDKIT_REPO_ROOT
# shellcheck source=scripts/lib/buildkit-artifact-common.sh
. "$SCRIPT_DIR/lib/buildkit-artifact-common.sh"

usage() {
  echo "usage: $0 --archive PATH --expected-sha256 SHA256 --output-dir DIR --source-mode MODE" >&2
  exit 2
}

archive=
expected_sha=
output_dir=
source_mode=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --archive) [ "$#" -ge 2 ] || usage; archive=$2; shift 2 ;;
    --expected-sha256) [ "$#" -ge 2 ] || usage; expected_sha=$2; shift 2 ;;
    --output-dir) [ "$#" -ge 2 ] || usage; output_dir=$2; shift 2 ;;
    --source-mode) [ "$#" -ge 2 ] || usage; source_mode=$2; shift 2 ;;
    *) usage ;;
  esac
done

[ -n "$archive" ] && [ -n "$expected_sha" ] && [ -n "$output_dir" ] && [ -n "$source_mode" ] || usage
case "$source_mode" in
  candidate-build|operator-override|published-download) ;;
  *) vz_buildkit_die "invalid source mode: $source_mode" ;;
esac
case "$expected_sha" in
  *[!0-9a-fA-F]*|'') vz_buildkit_die "expected SHA-256 must contain exactly 64 hexadecimal characters" ;;
esac
[ "${#expected_sha}" -eq 64 ] || vz_buildkit_die "expected SHA-256 must contain exactly 64 hexadecimal characters"
expected_sha=$(printf '%s' "$expected_sha" | tr 'A-F' 'a-f')
contract_archive_sha=$(vz_buildkit_contract_get archive_sha256)
[ "$expected_sha" = "$contract_archive_sha" ] || \
  vz_buildkit_die "expected SHA-256 does not match the pinned contract: expected $contract_archive_sha, received $expected_sha"
[ -f "$archive" ] && [ ! -L "$archive" ] || vz_buildkit_die "archive must be a regular non-symlink file: $archive"
command -v bsdtar >/dev/null 2>&1 || vz_buildkit_die "bsdtar is required"
command -v perl >/dev/null 2>&1 || vz_buildkit_die "perl is required"
vz_buildkit_prepare_fresh_dir "$output_dir"

actual_sha=$(vz_buildkit_sha256 "$archive")
[ "$actual_sha" = "$expected_sha" ] || vz_buildkit_die "archive checksum mismatch: expected $expected_sha, found $actual_sha"

work_dir="$output_dir/.validation-work"
mkdir -m 0700 "$work_dir"
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM
"$SCRIPT_DIR/lib/validate-buildkit-ustar.pl" "$archive" "$output_dir/buildkit-artifact-inventory.txt"
bsdtar -xf "$archive" -C "$work_dir"

vz_buildkit_write_manifest "$output_dir/manifest.json"
cmp -s "$output_dir/manifest.json" "$work_dir/manifest.json" || vz_buildkit_die "archive manifest does not match the pinned contract"

for binary in buildctl buildkitd; do
  path="$work_dir/bin/$binary"
  [ -f "$path" ] && [ ! -L "$path" ] || vz_buildkit_die "$binary is not a regular file"
  expected_binary_sha=$(vz_buildkit_contract_get "${binary}_sha256")
  actual_binary_sha=$(vz_buildkit_sha256 "$path")
  [ "$actual_binary_sha" = "$expected_binary_sha" ] || \
    vz_buildkit_die "$binary checksum mismatch: expected $expected_binary_sha, found $actual_binary_sha"
  "$SCRIPT_DIR/lib/validate-static-arm64-elf.pl" "$path"
done

archive_dir=$(CDPATH='' cd -- "$(dirname -- "$archive")" && pwd)
provenance="$archive_dir/buildkit-artifact-provenance.json"
if [ -e "$provenance" ]; then
  [ -f "$provenance" ] && [ ! -L "$provenance" ] || vz_buildkit_die "provenance must be a regular non-symlink file"
  contract_path=$(vz_buildkit_contract_path)
  contract_sha=$(vz_buildkit_sha256 "$contract_path")
  "$SCRIPT_DIR/lib/validate-buildkit-provenance.pl" \
    "$provenance" "$source_mode" "$actual_sha" "$contract_sha" \
    "$(vz_buildkit_contract_get source_commit)" \
    "$(vz_buildkit_contract_get buildctl_sha256)" \
    "$(vz_buildkit_contract_get buildkitd_sha256)"
  cp "$provenance" "$output_dir/buildkit-artifact-provenance.json"
elif [ "$source_mode" = candidate-build ]; then
  vz_buildkit_die "candidate-build provenance is required"
fi

verified_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
contract_path=$(vz_buildkit_contract_path)
contract_sha=$(vz_buildkit_sha256 "$contract_path")
validator_sha=$(vz_buildkit_sha256 "$SCRIPT_DIR/validate-runtime-free-buildkit.sh")
manifest_sha=$(vz_buildkit_sha256 "$work_dir/manifest.json")
manifest_size=$(wc -c < "$work_dir/manifest.json" | tr -d ' ')
buildctl_sha=$(vz_buildkit_sha256 "$work_dir/bin/buildctl")
buildctl_size=$(wc -c < "$work_dir/bin/buildctl" | tr -d ' ')
buildkitd_sha=$(vz_buildkit_sha256 "$work_dir/bin/buildkitd")
buildkitd_size=$(wc -c < "$work_dir/bin/buildkitd" | tr -d ' ')
cat > "$output_dir/buildkit-artifact-verification.json" <<EOF
{
  "schema_version": 1,
  "validator_version": 1,
  "validator_sha256": "$validator_sha",
  "verified_at_utc": "$verified_at",
  "source_mode": "$source_mode",
  "contract_sha256": "$contract_sha",
  "expected_archive_sha256": "$expected_sha",
  "actual_archive_sha256": "$actual_sha",
  "archive": {"format": "ustar", "entry_order_exact": true, "headers_canonical": true},
  "platform": "linux/arm64",
  "entries": [
    {"path": "manifest.json", "mode": "0644", "uid": 0, "gid": 0, "mtime": 0, "size": $manifest_size, "sha256": "$manifest_sha"},
    {"path": "bin/buildctl", "mode": "0755", "uid": 0, "gid": 0, "mtime": 0, "size": $buildctl_size, "sha256": "$buildctl_sha", "elf_class": "ELF64", "endianness": "little", "ident_version": 1, "osabi": "SYSV", "type": "ET_EXEC", "machine": "AArch64", "elf_version": 1, "program_headers_bounded": true, "pt_interp": false, "pt_dynamic": false},
    {"path": "bin/buildkitd", "mode": "0755", "uid": 0, "gid": 0, "mtime": 0, "size": $buildkitd_size, "sha256": "$buildkitd_sha", "elf_class": "ELF64", "endianness": "little", "ident_version": 1, "osabi": "SYSV", "type": "ET_EXEC", "machine": "AArch64", "elf_version": 1, "program_headers_bounded": true, "pt_interp": false, "pt_dynamic": false}
  ],
  "oci_runtime_binaries": [],
  "fallbacks": [],
  "retries": [],
  "verdict": "passed"
}
EOF
chmod 0644 "$output_dir"/*.json "$output_dir"/*.txt
rm -rf "$work_dir"
trap - EXIT HUP INT TERM
echo "validated runtime-free BuildKit archive: $archive"
