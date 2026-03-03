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
#   ./scripts/run-linux-btrfs-e2e-remote.sh --init-config
#   ./scripts/run-linux-btrfs-e2e-remote.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

HOST=""
WORKSPACE=""
REMOTE_REPO=""
REMOTE_REF=""
PROFILE=""
LOCAL_OUTPUT_ROOT=""
SSH_OPTS=()
CONFIG_PATH="${VZ_LINUX_BTRFS_E2E_CONFIG:-$REPO_ROOT/.config/vz-linux-btrfs-e2e.env}"
INIT_CONFIG=false

usage() {
    cat <<'USAGE'
run-linux-btrfs-e2e-remote.sh

Run Linux+btrfs portability evidence harness on a remote Linux host over SSH.

Options:
  --host <ssh-host>           SSH host (for example: user@linux-host)
  --workspace <path>          Remote btrfs workspace root
  --remote-repo <path>        Remote checkout path for this repo
  --ref <git-ref>             Git ref to checkout remotely (default: HEAD)
  --profile <debug|release>   Cargo profile (default: debug)
  --output-dir <path>         Local artifact root (default: .artifacts/linux-btrfs-e2e-remote)
  --config <path>             Config file path (default: .config/vz-linux-btrfs-e2e.env)
  --init-config               Write a starter config file and exit
  --ssh-opt <arg>             Additional ssh option (repeatable)
  -h, --help                  Show help

Notes:
  - The remote repo must be a valid git checkout with rust/cargo installed.
  - The remote host must have btrfs tooling and a writable btrfs workspace path.
  - Target must be a real remote Linux VM environment (not localhost/Docker-on-mac).
  - If no flags are passed, values are loaded from config/env.

Environment overrides:
  VZ_LINUX_BTRFS_E2E_HOST
  VZ_LINUX_BTRFS_E2E_WORKSPACE
  VZ_LINUX_BTRFS_E2E_REMOTE_REPO
  VZ_LINUX_BTRFS_E2E_REF
  VZ_LINUX_BTRFS_E2E_PROFILE
  VZ_LINUX_BTRFS_E2E_OUTPUT_DIR
  VZ_LINUX_BTRFS_E2E_CONFIG
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

validate_non_local_host() {
    local host="$1"
    local host_part="$host"
    if [[ "$host_part" == *"@"* ]]; then
        host_part="${host_part##*@}"
    fi
    if [[ "$host_part" == *":"* ]]; then
        host_part="${host_part%%:*}"
    fi

    case "${host_part,,}" in
        localhost|127.0.0.1|::1)
            err "localhost targets are not allowed for this harness; use a real remote vz Linux VM host"
            ;;
    esac
}

write_default_config() {
    local path="$1"
    mkdir -p "$(dirname "$path")"
    cat > "$path" <<'EOF'
# Required:
VZ_LINUX_BTRFS_E2E_HOST=user@vz-linux-host
VZ_LINUX_BTRFS_E2E_WORKSPACE=/mnt/vz-btrfs
VZ_LINUX_BTRFS_E2E_REMOTE_REPO=~/workspace/vz

# Optional:
# VZ_LINUX_BTRFS_E2E_REF=HEAD
# VZ_LINUX_BTRFS_E2E_PROFILE=debug
# VZ_LINUX_BTRFS_E2E_OUTPUT_DIR=.artifacts/linux-btrfs-e2e-remote
# VZ_LINUX_BTRFS_E2E_SSH_OPTS="-i ~/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new"
EOF
}

load_config_if_present() {
    local path="$1"
    if [[ -f "$path" ]]; then
        # shellcheck disable=SC1090
        source "$path"
    fi
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
        --config)
            CONFIG_PATH="${2:-}"
            shift 2
            ;;
        --init-config)
            INIT_CONFIG=true
            shift
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

if [[ "$INIT_CONFIG" == "true" ]]; then
    if [[ -f "$CONFIG_PATH" ]]; then
        err "config already exists at $CONFIG_PATH"
    fi
    write_default_config "$CONFIG_PATH"
    echo "wrote config template: $CONFIG_PATH"
    echo "edit values, then run: ./scripts/run-linux-btrfs-e2e-remote.sh"
    exit 0
fi

load_config_if_present "$CONFIG_PATH"

HOST="${HOST:-${VZ_LINUX_BTRFS_E2E_HOST:-}}"
WORKSPACE="${WORKSPACE:-${VZ_LINUX_BTRFS_E2E_WORKSPACE:-}}"
REMOTE_REPO="${REMOTE_REPO:-${VZ_LINUX_BTRFS_E2E_REMOTE_REPO:-}}"
REMOTE_REF="${REMOTE_REF:-${VZ_LINUX_BTRFS_E2E_REF:-HEAD}}"
PROFILE="${PROFILE:-${VZ_LINUX_BTRFS_E2E_PROFILE:-debug}}"
LOCAL_OUTPUT_ROOT="${LOCAL_OUTPUT_ROOT:-${VZ_LINUX_BTRFS_E2E_OUTPUT_DIR:-$REPO_ROOT/.artifacts/linux-btrfs-e2e-remote}}"

if [[ -n "${VZ_LINUX_BTRFS_E2E_SSH_OPTS:-}" ]]; then
    # shellcheck disable=SC2206
    ENV_SSH_OPTS=(${VZ_LINUX_BTRFS_E2E_SSH_OPTS})
    SSH_OPTS=("${ENV_SSH_OPTS[@]}" "${SSH_OPTS[@]}")
fi

[[ -n "$HOST" ]] || err "--host is required (flag, config, or VZ_LINUX_BTRFS_E2E_HOST)"
[[ -n "$WORKSPACE" ]] || err "--workspace is required (flag, config, or VZ_LINUX_BTRFS_E2E_WORKSPACE)"
[[ -n "$REMOTE_REPO" ]] || err "--remote-repo is required (flag, config, or VZ_LINUX_BTRFS_E2E_REMOTE_REPO)"
[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || err "--profile must be debug|release"
validate_non_local_host "$HOST"

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
