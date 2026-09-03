#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH='' cd -- "$SCRIPT_DIR/../.." && pwd)
VALIDATOR="$REPO_ROOT/scripts/validate-runtime-free-buildkit.sh"
BUILDER="$REPO_ROOT/scripts/build-runtime-free-buildkit.sh"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

expect_failure() {
  if "$@" >/dev/null 2>&1; then
    fail "command unexpectedly succeeded: $*"
  fi
}

command -v bsdtar >/dev/null 2>&1 || fail "bsdtar is required"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/vz-buildkit-artifact-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

# Minimal ELF64/AArch64 executable with one non-dynamic program header. The
# validator checks structure; the test contract pins these fixture bytes.
perl -e '
  print "\x7fELF", pack("C*", 2,1,1,0), "\0" x 8;
  print pack("v", 2), pack("v", 183), pack("V", 1);
  print pack("Q<", 0), pack("Q<", 64), pack("Q<", 0), pack("V", 0);
  print pack("v", 64), pack("v", 56), pack("v", 1), pack("v", 0), pack("v", 0), pack("v", 0);
  print pack("V", 1), "\0" x 52;
' > "$tmp/static-elf"
chmod 0755 "$tmp/static-elf"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi
}
binary_sha=$(sha256_file "$tmp/static-elf")
contract="$tmp/contract.json"
cat > "$contract" <<EOF
{
  "schema_version": 1,
  "buildkit_version": "0.19.0",
  "layout": 2,
  "platform": "linux/arm64",
  "source_repository": "https://example.invalid/buildkit.git",
  "source_tag": "v0.19.0",
  "source_commit": "3637d1b15a13fc3cdd0c16fcf3be0845ae68f53d",
  "go_version": "1.23.5",
  "go_darwin_arm64_sha256": "$binary_sha",
  "go_linux_amd64_sha256": "$binary_sha",
  "go_linux_arm64_sha256": "$binary_sha",
  "buildctl_sha256": "$binary_sha",
  "buildkitd_sha256": "$binary_sha",
  "archive_name": "fixture.tar",
  "archive_sha256": "$binary_sha",
  "inventory": ["manifest.json", "bin/buildctl", "bin/buildkitd"]
}
EOF
export VZ_BUILDKIT_CONTRACT="$contract"

package="$tmp/package"
mkdir -p "$package/bin"
cp "$tmp/static-elf" "$package/bin/buildctl"
cp "$tmp/static-elf" "$package/bin/buildkitd"
VZ_BUILDKIT_REPO_ROOT="$REPO_ROOT" . "$REPO_ROOT/scripts/lib/buildkit-artifact-common.sh"
vz_buildkit_write_manifest "$package/manifest.json"
chmod 0644 "$package/manifest.json"
chmod 0755 "$package/bin/buildctl" "$package/bin/buildkitd"
TZ=UTC touch -t 197001010000 "$package/manifest.json" "$package/bin/buildctl" "$package/bin/buildkitd"
archive="$tmp/fixture.tar"
LC_ALL=C TZ=UTC bsdtar -cf "$archive" --format=ustar --uid 0 --gid 0 --uname root --gname root \
  --no-xattrs --no-acls --no-fflags -C "$package" manifest.json bin/buildctl bin/buildkitd
archive_sha=$(sha256_file "$archive")

set_contract_archive_sha() {
  replacement=$1
  sed "s/\(\"archive_sha256\":[[:space:]]*\"\)[0-9a-f]*\"/\\1${replacement}\"/" "$contract" > "$contract.next"
  mv "$contract.next" "$contract"
}

set_contract_archive_sha "$archive_sha"

"$VALIDATOR" --archive "$archive" --expected-sha256 "$archive_sha" \
  --output-dir "$tmp/valid" --source-mode operator-override >/dev/null
[ "$(cat "$tmp/valid/buildkit-artifact-inventory.txt")" = "manifest.json
bin/buildctl
bin/buildkitd" ] || fail "validator emitted the wrong inventory"

expect_failure "$VALIDATOR" --archive "$archive" --expected-sha256 "${archive_sha%?}0" \
  --output-dir "$tmp/bad-sha" --source-mode operator-override
expect_failure "$VALIDATOR" --archive "$archive" --expected-sha256 "$archive_sha" \
  --output-dir "$tmp/bad-mode" --source-mode invalid-mode
mkdir "$tmp/not-empty"
touch "$tmp/not-empty/file"
expect_failure "$VALIDATOR" --archive "$archive" --expected-sha256 "$archive_sha" \
  --output-dir "$tmp/not-empty" --source-mode operator-override

extra="$tmp/extra.tar"
printf 'forbidden runtime\n' > "$package/bin/runc"
TZ=UTC touch -t 197001010000 "$package/bin/runc"
LC_ALL=C TZ=UTC bsdtar -cf "$extra" --format=ustar --uid 0 --gid 0 --uname root --gname root \
  --no-xattrs --no-acls --no-fflags -C "$package" manifest.json bin/buildctl bin/buildkitd bin/runc
extra_sha=$(sha256_file "$extra")
set_contract_archive_sha "$extra_sha"
expect_failure "$VALIDATOR" --archive "$extra" --expected-sha256 "$extra_sha" \
  --output-dir "$tmp/extra-result" --source-mode operator-override

make_bad_archive() {
  label=$1
  shift
  bad_archive="$tmp/$label.tar"
  LC_ALL=C TZ=UTC bsdtar -cf "$bad_archive" --format=ustar --uid 0 --gid 0 --uname root --gname root \
    --no-xattrs --no-acls --no-fflags -C "$package" "$@"
  bad_sha=$(sha256_file "$bad_archive")
  set_contract_archive_sha "$bad_sha"
  expect_failure "$VALIDATOR" --archive "$bad_archive" --expected-sha256 "$bad_sha" \
    --output-dir "$tmp/$label-result" --source-mode operator-override
}

rm "$package/bin/runc"
make_bad_archive reordered bin/buildctl manifest.json bin/buildkitd
chmod 0744 "$package/manifest.json"
make_bad_archive wrong-mode manifest.json bin/buildctl bin/buildkitd
chmod 0644 "$package/manifest.json"
TZ=UTC touch -t 197001010001 "$package/manifest.json"
make_bad_archive wrong-mtime manifest.json bin/buildctl bin/buildkitd
TZ=UTC touch -t 197001010000 "$package/manifest.json"

uid_archive="$tmp/wrong-uid.tar"
LC_ALL=C TZ=UTC bsdtar -cf "$uid_archive" --format=ustar --uid 1 --gid 0 --uname root --gname root \
  --no-xattrs --no-acls --no-fflags -C "$package" manifest.json bin/buildctl bin/buildkitd
uid_sha=$(sha256_file "$uid_archive")
set_contract_archive_sha "$uid_sha"
expect_failure "$VALIDATOR" --archive "$uid_archive" --expected-sha256 "$uid_sha" \
  --output-dir "$tmp/wrong-uid-result" --source-mode operator-override

trailing="$tmp/trailing.tar"
cp "$archive" "$trailing"
printf 'not-zero' >> "$trailing"
trailing_sha=$(sha256_file "$trailing")
set_contract_archive_sha "$trailing_sha"
expect_failure "$VALIDATOR" --archive "$trailing" --expected-sha256 "$trailing_sha" \
  --output-dir "$tmp/trailing-result" --source-mode operator-override

elf_validator="$REPO_ROOT/scripts/lib/validate-static-arm64-elf.pl"
for mutation in wrong-arch dynamic interp; do
  mutated_elf="$tmp/$mutation.elf"
  cp "$tmp/static-elf" "$mutated_elf"
  case "$mutation" in
    wrong-arch) perl -e 'open my $f, "+<:raw", $ARGV[0] or die $!; seek $f, 18, 0; print {$f} "\x3e\x00"' "$mutated_elf" ;;
    dynamic) perl -e 'open my $f, "+<:raw", $ARGV[0] or die $!; seek $f, 64, 0; print {$f} pack("V", 2)' "$mutated_elf" ;;
    interp) perl -e 'open my $f, "+<:raw", $ARGV[0] or die $!; seek $f, 64, 0; print {$f} pack("V", 3)' "$mutated_elf" ;;
  esac
  expect_failure "$elf_validator" "$mutated_elf"
done

expect_failure "$BUILDER"
expect_failure "$BUILDER" --unknown value
mkdir "$tmp/nonempty-builder"
touch "$tmp/nonempty-builder/file"
expect_failure "$BUILDER" --output-dir "$tmp/nonempty-builder"

echo "runtime-free BuildKit artifact tests passed"
