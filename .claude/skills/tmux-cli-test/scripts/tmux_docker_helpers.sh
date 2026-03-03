#!/usr/bin/env bash
# tmux_docker_helpers.sh - Condition-based tmux testing for CLI apps inside Docker containers.
# Source this file, set TMUX_DOCKER_CONTAINER, then use docker_tmux_* functions.
#
# Required: TMUX_DOCKER_CONTAINER - the Docker container name
# Optional: TMUX_DOCKER_SESSION (default: "test") - tmux session name inside the container
#
# Usage:
#   source .claude/skills/tmux-cli-test/scripts/tmux_docker_helpers.sh
#   TMUX_DOCKER_CONTAINER="gpu-ftr-alex-chen-001"
#   docker_tmux_send "gpu dashboard" Enter
#   docker_tmux_wait_for "Pods" 15
#   docker_tmux_capture
#
# Principle: NEVER sleep. Always wait on a condition with a timeout.

set -euo pipefail

# Source base helpers for defaults (TMUX_TEST_POLL_INTERVAL, TMUX_TEST_TIMEOUT, etc.)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/tmux_helpers.sh"

# Docker-specific config
TMUX_DOCKER_CONTAINER="${TMUX_DOCKER_CONTAINER:-}"
TMUX_DOCKER_SESSION="${TMUX_DOCKER_SESSION:-test}"

# Internal: run a tmux command inside the container.
_docker_tmux() {
    if [ -z "$TMUX_DOCKER_CONTAINER" ]; then
        echo "ERROR: TMUX_DOCKER_CONTAINER not set" >&2
        return 1
    fi
    docker exec "$TMUX_DOCKER_CONTAINER" tmux "$@"
}

# --- Send input ---

# Send keys to the tmux session inside the container.
# Usage: docker_tmux_send <keys...>
docker_tmux_send() {
    _docker_tmux send-keys -t "$TMUX_DOCKER_SESSION" "$@"
}

# Type literal text (no special key interpretation).
# Usage: docker_tmux_type <text>
docker_tmux_type() {
    _docker_tmux send-keys -t "$TMUX_DOCKER_SESSION" -l "$1"
}

# --- Capture ---

# Capture current pane as plain text.
# Usage: docker_tmux_capture
docker_tmux_capture() {
    _docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p
}

# Capture with ANSI escape codes.
# Usage: docker_tmux_capture_ansi
docker_tmux_capture_ansi() {
    _docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p -e
}

# --- Condition waits ---

# Wait until text appears. Returns 0 on match, 1 on timeout.
# Usage: docker_tmux_wait_for <text> [timeout_seconds]
docker_tmux_wait_for() {
    local text="$1"
    local timeout="${2:-$TMUX_TEST_TIMEOUT}"
    local elapsed=0

    while (( $(echo "$elapsed < $timeout" | bc -l) )); do
        if _docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p 2>/dev/null | grep -qF "$text"; then
            return 0
        fi
        sleep "$TMUX_TEST_POLL_INTERVAL"
        elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
    done
    echo "TIMEOUT: waited ${timeout}s for text: '$text'" >&2
    return 1
}

# Wait until regex matches. Returns 0 on match, 1 on timeout.
# Usage: docker_tmux_wait_regex <pattern> [timeout_seconds]
docker_tmux_wait_regex() {
    local pattern="$1"
    local timeout="${2:-$TMUX_TEST_TIMEOUT}"
    local elapsed=0

    while (( $(echo "$elapsed < $timeout" | bc -l) )); do
        if _docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p 2>/dev/null | grep -qE "$pattern"; then
            return 0
        fi
        sleep "$TMUX_TEST_POLL_INTERVAL"
        elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
    done
    echo "TIMEOUT: waited ${timeout}s for pattern: '$pattern'" >&2
    return 1
}

# Wait until text disappears. Returns 0 when gone, 1 on timeout.
# Usage: docker_tmux_wait_gone <text> [timeout_seconds]
docker_tmux_wait_gone() {
    local text="$1"
    local timeout="${2:-$TMUX_TEST_TIMEOUT}"
    local elapsed=0

    while (( $(echo "$elapsed < $timeout" | bc -l) )); do
        if ! _docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p 2>/dev/null | grep -qF "$text"; then
            return 0
        fi
        sleep "$TMUX_TEST_POLL_INTERVAL"
        elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
    done
    echo "TIMEOUT: waited ${timeout}s for text to disappear: '$text'" >&2
    return 1
}

# --- Assertions ---

# Assert pane contains text. Prints PASS/FAIL, returns 0/1.
# Usage: docker_tmux_assert_contains <text> [label]
docker_tmux_assert_contains() {
    local text="$1"
    local label="${2:-contains '$text'}"
    local frame
    frame=$(_docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p 2>/dev/null)

    if echo "$frame" | grep -qF "$text"; then
        echo "PASS: $label"
        return 0
    else
        echo "FAIL: $label"
        echo "--- captured frame ---"
        echo "$frame"
        echo "--- end frame ---"
        return 1
    fi
}

# Assert pane does NOT contain text.
# Usage: docker_tmux_assert_not_contains <text> [label]
docker_tmux_assert_not_contains() {
    local text="$1"
    local label="${2:-does not contain '$text'}"
    local frame
    frame=$(_docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p 2>/dev/null)

    if ! echo "$frame" | grep -qF "$text"; then
        echo "PASS: $label"
        return 0
    else
        echo "FAIL: $label"
        echo "--- captured frame ---"
        echo "$frame"
        echo "--- end frame ---"
        return 1
    fi
}

# Assert pane matches regex.
# Usage: docker_tmux_assert_matches <pattern> [label]
docker_tmux_assert_matches() {
    local pattern="$1"
    local label="${2:-matches '$pattern'}"
    local frame
    frame=$(_docker_tmux capture-pane -t "$TMUX_DOCKER_SESSION" -p 2>/dev/null)

    if echo "$frame" | grep -qE "$pattern"; then
        echo "PASS: $label"
        return 0
    else
        echo "FAIL: $label"
        echo "--- captured frame ---"
        echo "$frame"
        echo "--- end frame ---"
        return 1
    fi
}

# --- Compound helpers ---

# Send keys then wait for text to appear.
# Usage: docker_tmux_send_and_wait <keys> <expected_text> [timeout]
docker_tmux_send_and_wait() {
    local keys="$1"
    local expected="$2"
    local timeout="${3:-$TMUX_TEST_TIMEOUT}"
    _docker_tmux send-keys -t "$TMUX_DOCKER_SESSION" $keys
    docker_tmux_wait_for "$expected" "$timeout"
}
