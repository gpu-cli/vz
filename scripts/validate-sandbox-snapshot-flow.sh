#!/usr/bin/env bash
# Validate snapshot-backed sandbox attach/continue flow on macOS.
#
# This script drives interactive `vz` sessions via `expect` and verifies:
# 1) state continuity across `vz -c` and `vz attach <id>` using a /tmp marker file
# 2) corrupt snapshot fallback to cold boot (with warning)
# 3) snapshot artifact cleanup on `vz rm`
#
# Usage:
#   ./scripts/validate-sandbox-snapshot-flow.sh
#   ./scripts/validate-sandbox-snapshot-flow.sh --profile release

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROFILE="debug"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "error: snapshot flow validation requires macOS" >&2
    exit 1
fi

if [[ "$(uname -m)" != "arm64" ]]; then
    echo "error: snapshot flow validation requires Apple Silicon (arm64)" >&2
    exit 1
fi

if ! command -v expect >/dev/null 2>&1; then
    echo "error: expect is required (install via: brew install expect)" >&2
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "error: jq is required (install via: brew install jq)" >&2
    exit 1
fi

TARGET_DIR="$REPO_ROOT/crates/target/$PROFILE"
VZ_BIN="$TARGET_DIR/vz"
RUNTIMED_BIN="$TARGET_DIR/vz-runtimed"
ENTITLEMENTS="$REPO_ROOT/entitlements/vz-cli.entitlements.plist"
if [[ ! -x "$VZ_BIN" ]]; then
    echo "building vz binaries ($PROFILE)..." >&2
fi

(
    cd "$REPO_ROOT/crates"
    if [[ "$PROFILE" == "release" ]]; then
        cargo build --release -p vz-cli -p vz-guest-agent -p vz-runtimed
    else
        cargo build -p vz-cli -p vz-guest-agent -p vz-runtimed
    fi
)

"$REPO_ROOT/scripts/sign-dev.sh" --profile "$PROFILE"
if [[ -f "$RUNTIMED_BIN" ]]; then
    codesign --force --sign - --entitlements "$ENTITLEMENTS" "$RUNTIMED_BIN"
    codesign --verify --verbose "$RUNTIMED_BIN"
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="/tmp/vz-snapshot-validation-$timestamp"
HOME_DIR="$RUN_DIR/home"
WORK_DIR="$RUN_DIR/workspace"
mkdir -p "$HOME_DIR" "$WORK_DIR"
WORK_DIR_CANON="$(cd "$WORK_DIR" && pwd -P)"

SANDBOX_NAME="snapshot-proof-$timestamp"
SESSION_TIMEOUT=900
STATE_DB="$HOME_DIR/.vz/stack-state.db"
RUNTIME_DIR="$HOME_DIR/.vz/.vz-runtime"
SOCKET_PATH="$RUNTIME_DIR/runtimed.sock"
RUNTIMED_LOG="$RUN_DIR/runtimed.log"
HOST_KERNEL_DIR="${VZ_LINUX_DIR:-${HOME}/.vz/linux}"

echo "run_dir=$RUN_DIR"
echo "home_dir=$HOME_DIR"
echo "workspace=$WORK_DIR_CANON"
echo "binary=$VZ_BIN"

mkdir -p "$RUNTIME_DIR"
if [[ ! -d "$HOST_KERNEL_DIR" ]]; then
    echo "error: host kernel assets not found at $HOST_KERNEL_DIR" >&2
    exit 1
fi
mkdir -p "$HOME_DIR/.vz"
ln -sfn "$HOST_KERNEL_DIR" "$HOME_DIR/.vz/linux"

"$RUNTIMED_BIN" \
    --state-store-path "$STATE_DB" \
    --runtime-data-dir "$RUNTIME_DIR" \
    --socket-path "$SOCKET_PATH" \
    >"$RUNTIMED_LOG" 2>&1 &
RUNTIMED_PID=$!
trap 'kill "$RUNTIMED_PID" 2>/dev/null || true' EXIT

ready=false
for _ in {1..120}; do
    if [[ -S "$SOCKET_PATH" ]]; then
        ready=true
        break
    fi
    sleep 0.25
done
if [[ "$ready" != "true" ]]; then
    echo "error: vz-runtimed socket did not appear: $SOCKET_PATH" >&2
    echo "runtimed log follows:" >&2
    cat "$RUNTIMED_LOG" >&2 || true
    exit 1
fi

log_has_line() {
    local expected="$1"
    local file="$2"
    tr -d '\r' < "$file" | grep -Fxq "$expected"
}

run_expect_session_1() {
    local log="$RUN_DIR/session1-create.log"
    expect <<EOF
set timeout $SESSION_TIMEOUT
log_file -noappend "$log"
cd "$WORK_DIR"
spawn -noecho env HOME=$HOME_DIR $VZ_BIN --name $SANDBOX_NAME --cpus 2 --memory 2048
expect {
  -re "Sandbox ready" {}
  timeout {
    send_user "timeout waiting for sandbox readiness (session1)\n"
    exit 1
  }
}
send -- "echo SNAPSHOT_CONTINUITY_OK >/tmp/vz_snapshot_marker && cat /tmp/vz_snapshot_marker\r"
expect {
  -re "SNAPSHOT_CONTINUITY_OK" {}
  timeout {
    send_user "timeout waiting for marker write confirmation (session1)\n"
    exit 1
  }
}
send -- "echo SESSION1_DONE\r"
expect {
  -re "SESSION1_DONE" {}
  timeout {
    send_user "timeout waiting for session done marker (session1)\n"
    exit 1
  }
}
send -- "exit\r"
expect {
  eof {}
  timeout {
    send_user "timeout waiting for shell exit (session1)\n"
    exit 1
  }
}
EOF
    log_has_line "SESSION1_DONE" "$log"
}

run_expect_resume_continue() {
    local log="$RUN_DIR/session2-continue.log"
    expect <<EOF
set timeout $SESSION_TIMEOUT
log_file -noappend "$log"
cd "$WORK_DIR"
spawn -noecho env HOME=$HOME_DIR $VZ_BIN -c
expect {
  -re "Sandbox ready" {}
  timeout {
    send_user "timeout waiting for sandbox readiness (session2)\n"
    exit 1
  }
}
send -- {present="PRESENT"; missing="MISSING"; if [ -f /tmp/vz_snapshot_marker ] && /bin/busybox grep -q '^SNAPSHOT_CONTINUITY_OK$' /tmp/vz_snapshot_marker; then printf '\nSESSION2_MARKER_%s\n' "\$present"; else printf '\nSESSION2_MARKER_%s\n' "\$missing"; fi}
send -- "\r"
expect {
  -re "SESSION2_MARKER_PRESENT" {}
  timeout {
    send_user "timeout waiting for marker continuity result (session2)\n"
    exit 1
  }
}
send -- "exit\r"
expect {
  eof {}
  timeout {
    send_user "timeout waiting for shell exit (session2)\n"
    exit 1
  }
}
EOF
    log_has_line "SESSION2_MARKER_PRESENT" "$log"
}

run_expect_attach_by_id() {
    local sandbox_id="$1"
    local label="$2"
    local expect_alive="$3"
    local log="$RUN_DIR/${label}.log"
    local marker
    if [[ "$expect_alive" == "yes" ]]; then
        marker="SESSION3_MARKER_PRESENT"
    else
        marker="SESSION4_MARKER_MISSING"
    fi

    expect <<EOF
set timeout $SESSION_TIMEOUT
log_file -noappend "$log"
cd "$WORK_DIR"
spawn -noecho env HOME=$HOME_DIR $VZ_BIN attach $sandbox_id
expect {
  -re "Sandbox ready" {}
  timeout {
    send_user "timeout waiting for sandbox readiness ($label)\n"
    exit 1
  }
}
send -- {present="PRESENT"; missing="MISSING"; if [ -f /tmp/vz_snapshot_marker ] && /bin/busybox grep -q '^SNAPSHOT_CONTINUITY_OK$' /tmp/vz_snapshot_marker; then printf '\nSESSION3_MARKER_%s\n' "\$present"; else printf '\nSESSION4_MARKER_%s\n' "\$missing"; fi}
send -- "\r"
expect {
  -re "$marker" {}
  timeout {
    send_user "timeout waiting for marker continuity result ($label)\n"
    exit 1
  }
}
send -- "exit\r"
expect {
  eof {}
  timeout {
    send_user "timeout waiting for shell exit ($label)\n"
    exit 1
  }
}
EOF

    log_has_line "$marker" "$log"
}

run_expect_session_1
run_expect_resume_continue

sandbox_json="$(env HOME="$HOME_DIR" "$VZ_BIN" ls --json)"
sandbox_id="$(
    printf '%s\n' "$sandbox_json" \
        | jq -r --arg wd "$WORK_DIR_CANON" '.[] | select(.labels.project_dir == $wd) | .sandbox_id' \
        | tail -n 1
)"

if [[ -z "$sandbox_id" || "$sandbox_id" == "null" ]]; then
    echo "error: failed to resolve sandbox id from vz ls output" >&2
    exit 1
fi

snapshot_path="$HOME_DIR/.vz/.vz-runtime/sandboxes/${sandbox_id}.state"

run_expect_attach_by_id "$sandbox_id" "session3-attach" "yes"

if [[ ! -f "$snapshot_path" ]]; then
    echo "error: expected snapshot path to exist after attach session: $snapshot_path" >&2
    exit 1
fi

printf 'corrupt-snapshot' > "$snapshot_path"
run_expect_attach_by_id "$sandbox_id" "session4-attach-corrupt" "no"
grep -q "warning: failed to restore snapshot" "$RUN_DIR/session4-attach-corrupt.log"

if [[ ! -f "$snapshot_path" ]]; then
    echo "error: expected snapshot file to exist after session4 exit: $snapshot_path" >&2
    exit 1
fi

env HOME="$HOME_DIR" "$VZ_BIN" rm "$sandbox_id" | tee "$RUN_DIR/session5-rm.log"

if [[ -e "$snapshot_path" ]]; then
    echo "error: snapshot file still exists after vz rm: $snapshot_path" >&2
    exit 1
fi

cat > "$RUN_DIR/summary.txt" <<EOF
sandbox_id=$sandbox_id
snapshot_path=$snapshot_path
session1_log=$RUN_DIR/session1-create.log
session2_log=$RUN_DIR/session2-continue.log
session3_log=$RUN_DIR/session3-attach.log
session4_log=$RUN_DIR/session4-attach-corrupt.log
session5_log=$RUN_DIR/session5-rm.log
result=PASS
EOF

echo "snapshot validation passed"
echo "summary: $RUN_DIR/summary.txt"
