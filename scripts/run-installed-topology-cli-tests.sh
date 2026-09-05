#!/bin/bash
# DEV installed-artifact control-plane checks, not the physical 0.4 release gate.
set -euo pipefail

usage() {
    printf '%s\n' \
        'Usage: bash scripts/run-installed-topology-cli-tests.sh' \
        'Build/sign staged release CLI and daemon artifacts, then run all listed' \
        'black-box control-plane drivers against those exact executables.' \
        'Requires Apple-silicon macOS and local signing/socket permissions.' \
        'Does not replace the daily installation, run live Machine workloads,' \
        'or certify the complete Developer Environment release gate.'
}
if [[ $# -gt 0 ]]; then
    if [[ $# -eq 1 && ( "$1" == --help || "$1" == -h ) ]]; then
        usage
        exit 0
    fi
    usage >&2
    exit 2
fi

fail() { printf 'error: %s\n' "$*" >&2; exit 1; }
[[ "$(uname -s)" == Darwin && "$(uname -m)" == arm64 ]] || fail 'Apple-silicon macOS required'
for dependency in cargo jq codesign shasum git rg; do
    command -v "$dependency" >/dev/null || fail "missing dependency: $dependency"
done
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
drivers=(topology_up_cli topology_exec_cli topology_stop_cli topology_status_cli bare_help_cli legacy_cli_retirement api_http_mode_e2e)
driver_arguments=()
for driver in "${drivers[@]}"; do
    [[ -f "crates/vz-cli/tests/$driver.rs" ]] || fail "required driver missing: $driver"
    driver_arguments+=(--test "$driver")
done
umask 077
mkdir -p "$repo_root/.artifacts"
evidence="$(mktemp -d "$repo_root/.artifacts/topology-cli-installed-XXXXXX")"
printf 'DEV installed CLI evidence: %s\n' "$evidence"
cp "$repo_root/scripts/run-installed-topology-cli-tests.sh" "$evidence/harness.sh"
cp entitlements/vz-cli.entitlements.plist "$evidence/entitlements.plist"

# Bypass optional user-local compiler caches; do not change global settings.
export RUSTC_WRAPPER=
export CC_aarch64_apple_darwin=/usr/bin/clang
export CXX_aarch64_apple_darwin=/usr/bin/clang++
jq -n --arg commit "$(git rev-parse HEAD)" \
    --arg tracked_state "$(git status --porcelain=v1 --untracked-files=no)" \
    --arg scope 'DEV control-plane/retirement only; no physical Machine or full release claim' \
    '{schema_version:1,source_commit:$commit,tracked_state:$tracked_state,scope:$scope}' \
    > "$evidence/run-info.json"

cargo build --manifest-path crates/Cargo.toml --locked --release \
    -p vz-cli --bin vz -p vz-runtimed --bin vz-runtimed --message-format=json \
    2> >(tee "$evidence/build.log" >&2) | tee "$evidence/build.jsonl" >/dev/null

artifact_path() {
    jq -ser --arg target "$1" \
        '[.[] | select(.reason == "compiler-artifact" and .target.name == $target and .executable != null) | .executable] | unique | if length == 1 then .[0] else error("expected exactly one executable artifact") end' "$2"
}
for binary in vz vz-runtimed; do
    built="$(artifact_path "$binary" "$evidence/build.jsonl")"
    [[ "$built" == /* && -f "$built" && ! -L "$built" ]] || fail "invalid build artifact: $binary"
    cp "$built" "$evidence/$binary"
    codesign --force --sign - --entitlements entitlements/vz-cli.entitlements.plist "$evidence/$binary" \
        > "$evidence/$binary.sign.log" 2>&1
    codesign --verify --strict "$evidence/$binary" > "$evidence/$binary.verify.log" 2>&1
done
cargo test --manifest-path crates/Cargo.toml --locked -p vz-cli \
    "${driver_arguments[@]}" --no-run --message-format=json \
    2> >(tee "$evidence/driver-build.log" >&2) | tee "$evidence/driver-build.jsonl" >/dev/null

for driver in "${drivers[@]}"; do
    built="$(artifact_path "$driver" "$evidence/driver-build.jsonl")"
    [[ "$built" == /* && -f "$built" && ! -L "$built" ]] || fail "invalid test artifact: $driver"
    cp "$built" "$evidence/$driver"
    VZ_TEST_INSTALLED_CLI="$evidence/vz" VZ_TEST_INSTALLED_DAEMON="$evidence/vz-runtimed" \
        "$evidence/$driver" --nocapture --test-threads=1 2>&1 | tee "$evidence/$driver.log"
    # A zero-case/ignored/filtered run cannot certify even this narrow lane.
    [[ "$(rg -c '^test result:' "$evidence/$driver.log")" == 1 ]] || fail "invalid test summary: $driver"
    rg -q '^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;' \
        "$evidence/$driver.log" || fail "missing, skipped, filtered or failed tests: $driver"
done

# Bind all raw build/test/daemon logs and staged executables. No cleanup removes
# failed evidence; a failure above leaves its original directory for diagnosis.
files=()
while IFS= read -r file; do files+=("$file"); done < <(rg --files --hidden "$evidence" | LC_ALL=C sort)
shasum -a 256 "${files[@]}" > "$evidence/evidence.sha256"
shasum -a 256 -c "$evidence/evidence.sha256"
printf '%s\n' 'PASS: DEV installed CLI control-plane checks only; physical lifecycle/release gate remains separate.'
