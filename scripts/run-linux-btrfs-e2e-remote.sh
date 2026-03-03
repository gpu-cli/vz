#!/usr/bin/env bash
# Run Linux+btrfs release-gate tests on a real remote Linux environment via SSH.
#
# This wrapper is intended for dedicated vz-backed Linux environments where we
# execute Linux runtime validation. It runs scripts/run-linux-btrfs-e2e.sh on
# the remote host and copies artifacts back locally.
#
# Usage:
#   ./scripts/run-linux-btrfs-e2e-remote.sh \
#     --host ci-linux-01 \
#     --workspace /mnt/vz-btrfs \
#     --remote-repo ~/workspace/vz

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

HOST=""
WORKSPACE=""
REMOTE_REPO=""
REMOTE_REF="${REMOTE_REF:-HEAD}"
PROFILE="debug"
LOCAL_OUTPUT_ROOT="$REPO_ROOT/.artifacts/linux-btrfs-e2e-remote"
SSH_OPTS=()

usage() {
    cat <<'USAGE'
run-linux-btrfs-e2e-remote.sh

Run Linux+btrfs portability evidence harness on a remote Linux host over SSH.

Options:
  --host <ssh-host>           Required. SSH host (for example: user@linux-host)
  --workspace <path>          Required. Remote btrfs workspace root
  --remote-repo <path>        Required. Remote checkout path for this repo
  --ref <git-ref>             Git ref to checkout remotely (default: HEAD)
  --profile <debug|release>   Cargo profile (default: debug)
  --output-dir <path>         Local artifact root (default: .artifacts/linux-btrfs-e2e-remote)
  --ssh-opt <arg>             Additional ssh option (repeatable)
  -h, --help                  Show help

Notes:
  - The remote repo must be a valid git checkout with rust/cargo installed.
  - The remote host must have btrfs tooling and a writable btrfs workspace path.
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            HOST="${2:-}"
            shift 2
            ;;
        --workspace)
            WORKSPACE="${2:-}"
            shift 2
            ;;
        --remote-repo)
            REMOTE_REPO="${2:-}"
            shift 2
            ;;
        --ref)
            REMOTE_REF="${2:-}"
            shift 2
            ;;
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --output-dir)
            LOCAL_OUTPUT_ROOT="${2:-}"
            shift 2
            ;;
        --ssh-opt)
            SSH_OPTS+=("${2:-}")
            shift 2
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

[[ -n "$HOST" ]] || err "--host is required"
[[ -n "$WORKSPACE" ]] || err "--workspace is required"
[[ -n "$REMOTE_REPO" ]] || err "--remote-repo is required"
[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || err "--profile must be debug|release"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$LOCAL_OUTPUT_ROOT/$timestamp"
mkdir -p "$RUN_DIR"
ln -sfn "$timestamp" "$LOCAL_OUTPUT_ROOT/latest"

SSH_BASE=(ssh "${SSH_OPTS[@]}" "$HOST")
SCP_BASE=(scp "${SSH_OPTS[@]}")

local_ref="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
echo "==> remote host: $HOST"
echo "==> local ref: $local_ref"
echo "==> requested remote ref: $REMOTE_REF"
echo "==> local artifact dir: $RUN_DIR"

REMOTE_REPO_Q="$(printf '%q' "$REMOTE_REPO")"
REMOTE_REF_Q="$(printf '%q' "$REMOTE_REF")"
WORKSPACE_Q="$(printf '%q' "$WORKSPACE")"
PROFILE_Q="$(printf '%q' "$PROFILE")"

REMOTE_LATEST="$(
    "${SSH_BASE[@]}" \
        "REMOTE_REPO=$REMOTE_REPO_Q REMOTE_REF=$REMOTE_REF_Q WORKSPACE=$WORKSPACE_Q PROFILE=$PROFILE_Q bash -s" <<'EOF'
set -euo pipefail
cd "$REMOTE_REPO"
if ! command -v git >/dev/null 2>&1; then
  echo "error: git is not installed on remote host" >&2
  exit 2
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not installed on remote host" >&2
  exit 2
fi
if ! command -v btrfs >/dev/null 2>&1; then
  echo "error: btrfs command is not installed on remote host" >&2
  exit 2
fi
git fetch --all --tags --prune
git checkout "$REMOTE_REF"
VZ_TEST_BTRFS_WORKSPACE="$WORKSPACE" ./scripts/run-linux-btrfs-e2e.sh --workspace "$WORKSPACE" --profile "$PROFILE"
latest_target="$(readlink .artifacts/linux-btrfs-e2e/latest || true)"
if [[ -z "$latest_target" ]]; then
  echo "error: remote harness did not create latest artifact symlink" >&2
  exit 3
fi
echo "$latest_target"
EOF
)"
REMOTE_LATEST="${REMOTE_LATEST##*$'\n'}"
REMOTE_ARTIFACT_DIR="$REMOTE_REPO/.artifacts/linux-btrfs-e2e/$REMOTE_LATEST"
echo "==> remote artifacts: $REMOTE_ARTIFACT_DIR"

"${SCP_BASE[@]}" -r "$HOST:$REMOTE_ARTIFACT_DIR/" "$RUN_DIR/"

if [[ ! -f "$RUN_DIR/$(basename "$REMOTE_LATEST")/summary.txt" ]]; then
    err "failed to copy summary.txt from remote artifacts"
fi

echo "==> copied artifacts:"
echo "    $RUN_DIR/$(basename "$REMOTE_LATEST")"
echo
echo "==> summary"
cat "$RUN_DIR/$(basename "$REMOTE_LATEST")/summary.txt"
