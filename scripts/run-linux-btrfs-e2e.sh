#!/usr/bin/env bash
# Run supplemental Linux+btrfs real-storage tests for runtimed portability paths.
#
# This harness executes ignored tests that require:
# - Linux host
# - btrfs-provisioned workspace path
#
# Usage examples:
#   VZ_TEST_BTRFS_WORKSPACE=/mnt/vz-btrfs ./scripts/run-linux-btrfs-e2e.sh
#   ./scripts/run-linux-btrfs-e2e.sh --workspace /mnt/vz-btrfs --profile release
#   ./scripts/run-linux-btrfs-e2e.sh --keep-going

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROFILE="debug"
KEEP_GOING=false
OUTPUT_ROOT="$REPO_ROOT/.artifacts/linux-btrfs-e2e"
WORKSPACE="${VZ_TEST_BTRFS_WORKSPACE:-}"
RUN_ARGS=("--ignored" "--nocapture" "--test-threads=1")
TESTS=(
    "spaces_btrfs_checkpoint_restore_and_fork_use_real_subvolumes"
    "checkpoint_export_import_round_trip_preserves_workspace_snapshot"
    "space_cache_export_import_round_trip_preserves_payload"
)

usage() {
    cat <<'USAGE'
run-linux-btrfs-e2e.sh

Execute Linux+btrfs ignored runtimed tests and capture artifacts.

Options:
  --workspace <path>          btrfs workspace root (default: $VZ_TEST_BTRFS_WORKSPACE)
  --profile <debug|release>   Cargo profile (default: debug)
  --output-dir <path>         Artifact root (default: .artifacts/linux-btrfs-e2e)
  --keep-going                Continue after failures
  -h, --help                  Show help

Environment:
  VZ_TEST_BTRFS_WORKSPACE     Required if --workspace is not provided.
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

ensure_btrfs_workspace() {
    local path="$1"
    [[ -n "$path" ]] || err "VZ_TEST_BTRFS_WORKSPACE (or --workspace) is required"
    [[ -d "$path" ]] || err "workspace path is not a directory: $path"
    local fs_type
    fs_type="$(findmnt -n -T "$path" -o FSTYPE)"
    [[ "$fs_type" == "btrfs" ]] || err "workspace path must be on btrfs (detected: ${fs_type:-unknown})"
}

capture_owned_inventory() {
    local output_file="$1"
    {
        echo "[top-level-test-directories]"
        find "$WORKSPACE" -mindepth 1 -maxdepth 1 -type d \
            \( -name 'vz-space-*' -o -name 'vz-portability-*' -o -name 'vz-cache-portability-*' \) \
            -print | LC_ALL=C sort
        echo "[test-owned-subvolumes]"
        btrfs subvolume list "$WORKSPACE" \
            | awk '$0 ~ / path (.*\/)?vz-(space|portability|cache-portability)-[^\/]+(\/|$)/' \
            | LC_ALL=C sort
    } > "$output_file"
}

capture_cargo_source_manifest() {
    local output_file="$1"
    local source_list_file="$2"

    # The commit plus binary diff reconstruct all tracked inputs. This manifest additionally
    # binds every tracked or untracked regular file beneath Cargo's repository-local input roots,
    # so an untracked Rust/build/config input cannot influence the binary invisibly.
    git -C "$REPO_ROOT" ls-files --cached --others --exclude-standard -- \
        .cargo crates rust-toolchain rust-toolchain.toml \
        | LC_ALL=C sort -u > "$source_list_file"

    : > "$output_file"
    while IFS= read -r source_path; do
        [[ -n "$source_path" ]] || continue
        case "$source_path" in
            crates/.vz-runtime/*)
                # Local daemon state is not a Cargo input and may contain sockets/runtime data.
                continue
                ;;
        esac
        [[ -f "$REPO_ROOT/$source_path" ]] || continue
        (
            cd "$REPO_ROOT"
            sha256sum "$source_path"
        ) >> "$output_file"
    done < "$source_list_file"
}

write_sha256_manifest() {
    local run_dir="$1"
    local manifest="$run_dir/SHA256SUMS"
    (
        cd "$run_dir"
        for evidence_file in *; do
            [[ -f "$evidence_file" && "$evidence_file" != "SHA256SUMS" ]] || continue
            sha256sum "$evidence_file"
        done
    ) > "$manifest"
    (cd "$run_dir" && sha256sum -c SHA256SUMS)
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workspace)
            WORKSPACE="${2:-}"
            shift 2
            ;;
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_ROOT="${2:-}"
            shift 2
            ;;
        --keep-going)
            KEEP_GOING=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            err "unknown argument '$1'"
            ;;
    esac
done

[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || err "--profile must be debug|release"
[[ "$(uname -s)" == "Linux" ]] || err "this harness must run on Linux"
command -v btrfs >/dev/null 2>&1 || err "btrfs command not found in PATH"
command -v findmnt >/dev/null 2>&1 || err "findmnt command not found in PATH"
command -v git >/dev/null 2>&1 || err "git command not found in PATH"
command -v sha256sum >/dev/null 2>&1 || err "sha256sum command not found in PATH"

ensure_btrfs_workspace "$WORKSPACE"
WORKSPACE="$(readlink -f "$WORKSPACE")"
export VZ_TEST_BTRFS_WORKSPACE="$WORKSPACE"
: "${CARGO_TARGET_DIR:=$WORKSPACE/.cargo-target}"
export CARGO_TARGET_DIR
mkdir -p "$CARGO_TARGET_DIR"
echo "==> disk usage preflight"
df -h "$WORKSPACE" "$CARGO_TARGET_DIR"
btrfs filesystem usage -T "$WORKSPACE"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTPUT_ROOT"
OUTPUT_ROOT="$(cd "$OUTPUT_ROOT" && pwd)"
RUN_DIR="$OUTPUT_ROOT/$timestamp"
mkdir "$RUN_DIR" || err "artifact run directory already exists: $RUN_DIR"
ln -sfn "$timestamp" "$OUTPUT_ROOT/latest"

BUILD_ARGS=()
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi

echo "==> output directory: $RUN_DIR"
FAILED=()
PASSED=()

git -C "$REPO_ROOT" diff --binary HEAD > "$RUN_DIR/git-diff.patch"
git -C "$REPO_ROOT" status --porcelain=v1 --untracked-files=all > "$RUN_DIR/git-status.txt"
capture_cargo_source_manifest \
    "$RUN_DIR/cargo-source-manifest.txt" \
    "$RUN_DIR/cargo-source-paths.txt"
GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
GIT_DIFF_SHA256="$(sha256sum "$RUN_DIR/git-diff.patch" | awk '{print $1}')"
GIT_STATUS_SHA256="$(sha256sum "$RUN_DIR/git-status.txt" | awk '{print $1}')"
CARGO_SOURCE_MANIFEST_SHA256="$(sha256sum "$RUN_DIR/cargo-source-manifest.txt" | awk '{print $1}')"

{
    uname -a
    cat /proc/version
    cat /proc/cmdline
} > "$RUN_DIR/kernel.txt"
findmnt -n -T "$WORKSPACE" -o TARGET,SOURCE,FSTYPE,FSROOT,UUID,OPTIONS \
    > "$RUN_DIR/workspace-mount.txt"
btrfs filesystem show --raw "$WORKSPACE" > "$RUN_DIR/btrfs-filesystem.txt"
btrfs device stats "$WORKSPACE" > "$RUN_DIR/btrfs-device-stats-before.txt"
capture_owned_inventory "$RUN_DIR/test-owned-inventory-before.txt"

BUILD_LOG="$RUN_DIR/test-build.jsonl"
build_cmd=(cargo test -p vz-runtimed "${BUILD_ARGS[@]}" --lib --no-run --message-format=json-render-diagnostics)
echo "building release-gate test binary: ${build_cmd[*]}"
set +e
(
    cd "$REPO_ROOT/crates"
    "${build_cmd[@]}"
) 2>&1 | tee "$BUILD_LOG"
build_pipeline_status=("${PIPESTATUS[@]}")
set -e
build_status="${build_pipeline_status[0]}"
build_tee_status="${build_pipeline_status[1]}"
if [[ $build_status -ne 0 ]]; then
    FAILED+=("test-build:$build_status")
fi
if [[ $build_tee_status -ne 0 ]]; then
    FAILED+=("test-build-log:$build_tee_status")
fi

TEST_BINARY=""
if [[ ${#FAILED[@]} -eq 0 ]]; then
    TEST_BINARY_CANDIDATES="$RUN_DIR/test-binary-candidates.txt"
    awk '
        /"reason":"compiler-artifact"/ &&
        /"kind":\["lib"\]/ &&
        /"name":"vz_runtimed"/ &&
        /"executable":"/ {
            line = $0
            sub(/^.*"executable":"/, "", line)
            sub(/".*$/, "", line)
            print line
        }
    ' "$BUILD_LOG" | LC_ALL=C sort -u > "$TEST_BINARY_CANDIDATES"

    test_binary_candidates=()
    while IFS= read -r candidate; do
        [[ -n "$candidate" ]] && test_binary_candidates+=("$candidate")
    done < "$TEST_BINARY_CANDIDATES"
    if [[ ${#test_binary_candidates[@]} -ne 1 ]]; then
        FAILED+=("test-binary-resolution:87")
        echo "error: expected exactly one vz-runtimed library test binary, found ${#test_binary_candidates[@]}" >&2
    else
        TEST_BINARY="${test_binary_candidates[0]}"
        if [[ ! -x "$TEST_BINARY" ]]; then
            FAILED+=("test-binary-resolution:87")
            echo "error: resolved test binary is not executable: $TEST_BINARY" >&2
        fi
    fi
fi

TEST_BINARY_SHA256="none"
if [[ -n "$TEST_BINARY" && -x "$TEST_BINARY" ]]; then
    TEST_BINARY_SHA256="$(sha256sum "$TEST_BINARY" | awk '{print $1}')"
fi

MOUNT_TARGET="$(findmnt -n -T "$WORKSPACE" -o TARGET)"
MOUNT_SOURCE="$(findmnt -n -T "$WORKSPACE" -o SOURCE)"
MOUNT_FSTYPE="$(findmnt -n -T "$WORKSPACE" -o FSTYPE)"
MOUNT_FSROOT="$(findmnt -n -T "$WORKSPACE" -o FSROOT)"
MOUNT_UUID="$(findmnt -n -T "$WORKSPACE" -o UUID)"
{
    echo "schema_version=1"
    echo "timestamp_utc=$timestamp"
    echo "host=$(hostname)"
    echo "profile=$PROFILE"
    echo "git_commit=$GIT_COMMIT"
    echo "git_diff_sha256=$GIT_DIFF_SHA256"
    echo "git_status_sha256=$GIT_STATUS_SHA256"
    echo "cargo_source_manifest_sha256=$CARGO_SOURCE_MANIFEST_SHA256"
    echo "test_binary=$TEST_BINARY"
    echo "test_binary_sha256=$TEST_BINARY_SHA256"
    echo "kernel_release=$(uname -r)"
    echo "kernel_machine=$(uname -m)"
    echo "workspace=$WORKSPACE"
    echo "workspace_mount_target=$MOUNT_TARGET"
    echo "workspace_mount_source=$MOUNT_SOURCE"
    echo "workspace_mount_fstype=$MOUNT_FSTYPE"
    echo "workspace_mount_fsroot=$MOUNT_FSROOT"
    echo "workspace_mount_uuid=$MOUNT_UUID"
    echo "tests=${TESTS[*]}"
    echo "evidence_scope=btrfs-storage-only"
    echo "runtime_backend=test"
    echo "production_linux_native_backend_claim=not-covered"
    echo "oci_runtime_claim=not-applicable-storage-only"
} > "$RUN_DIR/run-info.txt"

if [[ ${#FAILED[@]} -eq 0 ]]; then
    for test_name in "${TESTS[@]}"; do
        test_filter="grpc::tests::$test_name"
        log_file="$RUN_DIR/${test_name}.log"
        cmd=("$TEST_BINARY" "${RUN_ARGS[@]}" --exact "$test_filter")
        echo "running [$test_name]: ${cmd[*]}"

        set +e
        "${cmd[@]}" 2>&1 | tee "$log_file"
        pipeline_status=("${PIPESTATUS[@]}")
        set -e
        status="${pipeline_status[0]}"
        tee_status="${pipeline_status[1]}"

        if [[ $tee_status -ne 0 ]]; then
            status=94
            echo "error: artifact log capture failed for '$test_name' (tee exit $tee_status)" >&2
        fi
        if [[ $status -eq 0 ]] \
            && ! grep -Fqx "running 1 test" "$log_file"; then
            status=86
            echo "error: exact test '$test_filter' did not report one selected test" >&2
        fi
        if [[ $status -eq 0 ]] \
            && ! grep -Eq \
                '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [^[:space:]]+$' \
                "$log_file"; then
            status=88
            echo "error: exact test '$test_filter' did not report one pass with zero failed/ignored" >&2
        fi

        if [[ $status -eq 0 ]]; then
            PASSED+=("$test_name")
        else
            FAILED+=("$test_name:$status")
            if [[ "$KEEP_GOING" != "true" ]]; then
                break
            fi
        fi
    done
fi

capture_owned_inventory "$RUN_DIR/test-owned-inventory-after.txt"
btrfs device stats "$WORKSPACE" > "$RUN_DIR/btrfs-device-stats-after.txt"
btrfs filesystem show --raw "$WORKSPACE" > "$RUN_DIR/btrfs-filesystem-after.txt"
findmnt -n -T "$WORKSPACE" -o TARGET,SOURCE,FSTYPE,FSROOT,UUID,OPTIONS \
    > "$RUN_DIR/workspace-mount-after.txt"
if ! cmp -s \
    "$RUN_DIR/test-owned-inventory-before.txt" \
    "$RUN_DIR/test-owned-inventory-after.txt"; then
    FAILED+=("test-owned-inventory-cleanup:89")
    set +e
    diff -u \
        "$RUN_DIR/test-owned-inventory-before.txt" \
        "$RUN_DIR/test-owned-inventory-after.txt" \
        > "$RUN_DIR/test-owned-inventory.diff"
    diff_status=$?
    set -e
    if [[ $diff_status -ne 1 ]]; then
        FAILED+=("test-owned-inventory-diff:$diff_status")
    fi
fi
if ! cmp -s "$RUN_DIR/workspace-mount.txt" "$RUN_DIR/workspace-mount-after.txt"; then
    FAILED+=("workspace-mount-identity-changed:90")
fi
if ! cmp -s \
    "$RUN_DIR/btrfs-device-stats-before.txt" \
    "$RUN_DIR/btrfs-device-stats-after.txt"; then
    FAILED+=("btrfs-device-stats-changed:91")
fi

git -C "$REPO_ROOT" diff --binary HEAD > "$RUN_DIR/git-diff-after.patch"
git -C "$REPO_ROOT" status --porcelain=v1 --untracked-files=all \
    > "$RUN_DIR/git-status-after.txt"
capture_cargo_source_manifest \
    "$RUN_DIR/cargo-source-manifest-after.txt" \
    "$RUN_DIR/cargo-source-paths-after.txt"
POST_GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
POST_GIT_DIFF_SHA256="$(sha256sum "$RUN_DIR/git-diff-after.patch" | awk '{print $1}')"
POST_GIT_STATUS_SHA256="$(sha256sum "$RUN_DIR/git-status-after.txt" | awk '{print $1}')"
POST_CARGO_SOURCE_MANIFEST_SHA256="$(sha256sum "$RUN_DIR/cargo-source-manifest-after.txt" | awk '{print $1}')"
POST_TEST_BINARY_SHA256="none"
if [[ -n "$TEST_BINARY" && -x "$TEST_BINARY" ]]; then
    POST_TEST_BINARY_SHA256="$(sha256sum "$TEST_BINARY" | awk '{print $1}')"
fi
if [[ "$POST_GIT_COMMIT" != "$GIT_COMMIT" \
    || "$POST_GIT_DIFF_SHA256" != "$GIT_DIFF_SHA256" \
    || "$POST_GIT_STATUS_SHA256" != "$GIT_STATUS_SHA256" \
    || "$POST_CARGO_SOURCE_MANIFEST_SHA256" != "$CARGO_SOURCE_MANIFEST_SHA256" ]]; then
    FAILED+=("source-provenance-changed-during-run:92")
fi
if [[ "$POST_TEST_BINARY_SHA256" != "$TEST_BINARY_SHA256" ]]; then
    FAILED+=("test-binary-changed-during-run:93")
fi

{
    echo "passed=${PASSED[*]:-none}"
    echo "failed=${FAILED[*]:-none}"
    echo "test_binary_sha256=$TEST_BINARY_SHA256"
    echo "git_commit=$GIT_COMMIT"
    echo "git_diff_sha256=$GIT_DIFF_SHA256"
    echo "cargo_source_manifest_sha256=$CARGO_SOURCE_MANIFEST_SHA256"
    echo "post_git_commit=$POST_GIT_COMMIT"
    echo "post_git_diff_sha256=$POST_GIT_DIFF_SHA256"
    echo "post_cargo_source_manifest_sha256=$POST_CARGO_SOURCE_MANIFEST_SHA256"
    echo "post_test_binary_sha256=$POST_TEST_BINARY_SHA256"
    echo "evidence_scope=btrfs-storage-only"
    echo "runtime_backend=test"
    echo "production_linux_native_backend_claim=not-covered"
    echo "manifest=$RUN_DIR/SHA256SUMS"
} > "$RUN_DIR/summary.txt"

write_sha256_manifest "$RUN_DIR" || err "SHA-256 manifest creation or verification failed"

echo "==> summary"
echo "passed: ${PASSED[*]:-none}"
echo "failed: ${FAILED[*]:-none}"
echo "evidence manifest: $RUN_DIR/SHA256SUMS"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    exit 1
fi
