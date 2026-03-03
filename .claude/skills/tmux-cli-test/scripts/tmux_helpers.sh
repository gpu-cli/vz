#!/usr/bin/env bash
# tmux_helpers.sh - Condition-based tmux testing primitives for CLI applications.
# Source this file, then use the functions below.
#
# Principle: NEVER sleep. Always wait on a condition with a timeout.

set -euo pipefail

# Defaults (override before calling functions)
TMUX_TEST_POLL_INTERVAL="${TMUX_TEST_POLL_INTERVAL:-0.3}"
TMUX_TEST_TIMEOUT="${TMUX_TEST_TIMEOUT:-30}"
TMUX_TEST_WIDTH="${TMUX_TEST_WIDTH:-120}"
TMUX_TEST_HEIGHT="${TMUX_TEST_HEIGHT:-30}"

# --- Session lifecycle ---

# Create a tmux session running a command.
# Usage: tmux_start <session-name> <command...>
tmux_start() {
    local session="$1"; shift
    local cmd="$*"
    # Kill stale session if exists
    tmux kill-session -t "$session" 2>/dev/null || true
    tmux new-session -d -s "$session" \
        -x "$TMUX_TEST_WIDTH" -y "$TMUX_TEST_HEIGHT" \
        "$cmd"
}

# Kill a tmux session.
# Usage: tmux_kill <session-name>
tmux_kill() {
    local session="$1"
    tmux kill-session -t "$session" 2>/dev/null || true
}

# Check if a session is still running.
# Usage: tmux_is_alive <session-name>
tmux_is_alive() {
    local session="$1"
    tmux has-session -t "$session" 2>/dev/null
}

# --- Capture ---

# Capture the current pane contents as plain text.
# Usage: tmux_capture <session-name>
tmux_capture() {
    local session="$1"
    tmux capture-pane -t "$session" -p
}

# Capture with ANSI escape codes (for color verification).
# Usage: tmux_capture_ansi <session-name>
tmux_capture_ansi() {
    local session="$1"
    tmux capture-pane -t "$session" -p -e
}

# Capture and save to a file, return the path.
# Usage: tmux_capture_to_file <session-name> [filename]
tmux_capture_to_file() {
    local session="$1"
    local file="${2:-/tmp/tmux-capture-${session}-$(date +%s).txt}"
    tmux capture-pane -t "$session" -p > "$file"
    echo "$file"
}

# --- Condition waits (NO sleeps) ---

# Wait until text appears in the pane. Returns 0 on match, 1 on timeout.
# Usage: tmux_wait_for <session-name> <text> [timeout_seconds]
tmux_wait_for() {
    local session="$1"
    local text="$2"
    local timeout="${3:-$TMUX_TEST_TIMEOUT}"
    local elapsed=0

    while (( $(echo "$elapsed < $timeout" | bc -l) )); do
        if tmux capture-pane -t "$session" -p 2>/dev/null | grep -qF "$text"; then
            return 0
        fi
        sleep "$TMUX_TEST_POLL_INTERVAL"
        elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
    done
    echo "TIMEOUT: waited ${timeout}s for text: '$text'" >&2
    return 1
}

# Wait until a regex matches in the pane. Returns 0 on match, 1 on timeout.
# Usage: tmux_wait_for_regex <session-name> <pattern> [timeout_seconds]
tmux_wait_for_regex() {
    local session="$1"
    local pattern="$2"
    local timeout="${3:-$TMUX_TEST_TIMEOUT}"
    local elapsed=0

    while (( $(echo "$elapsed < $timeout" | bc -l) )); do
        if tmux capture-pane -t "$session" -p 2>/dev/null | grep -qE "$pattern"; then
            return 0
        fi
        sleep "$TMUX_TEST_POLL_INTERVAL"
        elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
    done
    echo "TIMEOUT: waited ${timeout}s for pattern: '$pattern'" >&2
    return 1
}

# Wait until text disappears from the pane. Returns 0 when gone, 1 on timeout.
# Usage: tmux_wait_gone <session-name> <text> [timeout_seconds]
tmux_wait_gone() {
    local session="$1"
    local text="$2"
    local timeout="${3:-$TMUX_TEST_TIMEOUT}"
    local elapsed=0

    while (( $(echo "$elapsed < $timeout" | bc -l) )); do
        if ! tmux capture-pane -t "$session" -p 2>/dev/null | grep -qF "$text"; then
            return 0
        fi
        sleep "$TMUX_TEST_POLL_INTERVAL"
        elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
    done
    echo "TIMEOUT: waited ${timeout}s for text to disappear: '$text'" >&2
    return 1
}

# Wait until the session exits (process completes). Returns 0 when exited, 1 on timeout.
# Usage: tmux_wait_exit <session-name> [timeout_seconds]
tmux_wait_exit() {
    local session="$1"
    local timeout="${2:-$TMUX_TEST_TIMEOUT}"
    local elapsed=0

    while (( $(echo "$elapsed < $timeout" | bc -l) )); do
        if ! tmux has-session -t "$session" 2>/dev/null; then
            return 0
        fi
        sleep "$TMUX_TEST_POLL_INTERVAL"
        elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
    done
    echo "TIMEOUT: waited ${timeout}s for session '$session' to exit" >&2
    return 1
}

# --- Send input ---

# Send keys to a tmux session.
# Usage: tmux_send <session-name> <keys...>
# Examples:
#   tmux_send mysession "hello world" Enter
#   tmux_send mysession j j j Enter
#   tmux_send mysession Escape
#   tmux_send mysession C-c
tmux_send() {
    local session="$1"; shift
    tmux send-keys -t "$session" "$@"
}

# Type text (sends literal string, no special key interpretation).
# Usage: tmux_type <session-name> <text>
tmux_type() {
    local session="$1"
    local text="$2"
    tmux send-keys -t "$session" -l "$text"
}

# --- Assertions ---

# Assert the pane contains text. Prints PASS/FAIL, returns 0/1.
# Usage: tmux_assert_contains <session-name> <text> [label]
tmux_assert_contains() {
    local session="$1"
    local text="$2"
    local label="${3:-contains '$text'}"
    local frame
    frame=$(tmux capture-pane -t "$session" -p 2>/dev/null)

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

# Assert the pane does NOT contain text.
# Usage: tmux_assert_not_contains <session-name> <text> [label]
tmux_assert_not_contains() {
    local session="$1"
    local text="$2"
    local label="${3:-does not contain '$text'}"
    local frame
    frame=$(tmux capture-pane -t "$session" -p 2>/dev/null)

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

# Assert the pane matches a regex.
# Usage: tmux_assert_matches <session-name> <pattern> [label]
tmux_assert_matches() {
    local session="$1"
    local pattern="$2"
    local label="${3:-matches '$pattern'}"
    local frame
    frame=$(tmux capture-pane -t "$session" -p 2>/dev/null)

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

# Send keys then wait for expected text to appear.
# Usage: tmux_send_and_wait <session-name> <keys> <expected_text> [timeout]
tmux_send_and_wait() {
    local session="$1"
    local keys="$2"
    local expected="$3"
    local timeout="${4:-$TMUX_TEST_TIMEOUT}"
    tmux send-keys -t "$session" $keys
    tmux_wait_for "$session" "$expected" "$timeout"
}

# Run a full test: start session, wait for ready text, execute steps, cleanup.
# Usage: tmux_test <session-name> <command> <ready_text> <test_function>
# The test_function receives the session name as $1.
tmux_test() {
    local session="$1"
    local cmd="$2"
    local ready_text="$3"
    local test_fn="$4"

    echo "=== Test: $session ==="
    tmux_start "$session" "$cmd"

    if ! tmux_wait_for "$session" "$ready_text"; then
        echo "FAIL: session never became ready (expected: '$ready_text')"
        tmux_capture "$session"
        tmux_kill "$session"
        return 1
    fi

    local result=0
    "$test_fn" "$session" || result=$?

    tmux_kill "$session"
    if [ "$result" -eq 0 ]; then
        echo "=== PASSED: $session ==="
    else
        echo "=== FAILED: $session ==="
    fi
    return "$result"
}

# --- Screenshots (requires: freeze — brew install charmbracelet/tap/freeze) ---

# Screenshot output directory (override to change).
TMUX_SCREENSHOT_DIR="${TMUX_SCREENSHOT_DIR:-/tmp/tui-screenshots}"

# Take a PNG screenshot of the current pane using freeze.
# Usage: tmux_screenshot <session-name> <label>
# Returns: path to PNG file. Prints path to stdout.
# Example: tmux_screenshot mysession "01-idle-state"
tmux_screenshot() {
    local session="$1"
    local label="${2:-capture}"
    mkdir -p "$TMUX_SCREENSHOT_DIR"
    local outfile="${TMUX_SCREENSHOT_DIR}/${session}-${label}.png"

    if ! command -v freeze &>/dev/null; then
        echo "SKIP: freeze not installed (brew install charmbracelet/tap/freeze)" >&2
        return 1
    fi

    tmux capture-pane -t "$session" -p -e \
        | freeze -c full --window=false -o "$outfile" --language bash 2>/dev/null

    if [ -f "$outfile" ]; then
        echo "$outfile"
    else
        echo "FAIL: screenshot not created" >&2
        return 1
    fi
}

# Take a screenshot with window chrome (macOS-style title bar).
# Usage: tmux_screenshot_framed <session-name> <label>
tmux_screenshot_framed() {
    local session="$1"
    local label="${2:-capture}"
    mkdir -p "$TMUX_SCREENSHOT_DIR"
    local outfile="${TMUX_SCREENSHOT_DIR}/${session}-${label}.png"

    if ! command -v freeze &>/dev/null; then
        echo "SKIP: freeze not installed" >&2
        return 1
    fi

    tmux capture-pane -t "$session" -p -e \
        | freeze -c full --window -o "$outfile" --language bash 2>/dev/null

    echo "$outfile"
}

# Take screenshots at multiple terminal sizes for layout review.
# Usage: tmux_screenshot_sizes <session-name> <label-prefix>
# Captures at 80x24, 120x30, 160x40, 200x50 and returns all paths.
tmux_screenshot_sizes() {
    local session="$1"
    local prefix="${2:-resize}"
    local sizes=("80 24" "120 30" "160 40" "200 50")
    local paths=()

    for size in "${sizes[@]}"; do
        read -r w h <<< "$size"
        tmux resize-pane -t "$session" -x "$w" -y "$h" 2>/dev/null || continue
        # Wait for re-render
        local elapsed=0
        while (( $(echo "$elapsed < 2" | bc -l) )); do
            sleep "$TMUX_TEST_POLL_INTERVAL"
            elapsed=$(echo "$elapsed + $TMUX_TEST_POLL_INTERVAL" | bc -l)
            # Check if session still alive after resize
            tmux_is_alive "$session" || break
        done
        local path
        path=$(tmux_screenshot "$session" "${prefix}-${w}x${h}")
        paths+=("$path")
    done

    # Restore original size
    tmux resize-pane -t "$session" -x "$TMUX_TEST_WIDTH" -y "$TMUX_TEST_HEIGHT" 2>/dev/null || true

    printf '%s\n' "${paths[@]}"
}

# --- Color assertions (ANSI-based) ---

# Check if ANY ANSI color code is present in the pane output.
# Usage: tmux_assert_has_color <session> <ansi-code> [label]
# ansi-code examples: "32" (green), "31" (red), "38;2;0;200;83" (exact RGB)
tmux_assert_has_color() {
    local session="$1"
    local code="$2"
    local label="${3:-has ANSI color $code}"
    local esc=$'\033'
    local frame
    frame=$(tmux capture-pane -t "$session" -p -e 2>/dev/null)

    if printf '%s' "$frame" | grep -qF "${esc}[${code}m"; then
        echo "PASS: $label"
        return 0
    fi
    # Also try as part of a compound code (e.g. ESC[1;32m)
    if printf '%s' "$frame" | grep -qF ";${code}m"; then
        echo "PASS: $label"
        return 0
    fi
    echo "FAIL: $label"
    return 1
}

# Check that specific text appears on a line that also has a specific color.
# Usage: tmux_assert_text_color <session> <text> <ansi-code> [label]
# Example: tmux_assert_text_color "$S" "READY" "32" "READY is green"
tmux_assert_text_color() {
    local session="$1"
    local text="$2"
    local code="$3"
    local label="${4:-'$text' colored with $code}"
    local esc=$'\033'
    local frame
    frame=$(tmux capture-pane -t "$session" -p -e 2>/dev/null)

    # Find lines containing the text, check if any have the color code
    local matching_lines
    matching_lines=$(printf '%s' "$frame" | grep -F "$text" || true)

    if [ -z "$matching_lines" ]; then
        echo "FAIL: $label (text '$text' not found)"
        return 1
    fi

    if printf '%s' "$matching_lines" | grep -qF "${code}"; then
        echo "PASS: $label"
        return 0
    fi
    echo "FAIL: $label (text found but color $code not on same line)"
    return 1
}

# Check that the pane uses at least N distinct colors (measures color richness).
# Excludes reset (0m) from the count — only real color/style codes are counted.
# Usage: tmux_assert_min_colors <session> <min_count> [label]
tmux_assert_min_colors() {
    local session="$1"
    local min_count="$2"
    local label="${3:-at least $min_count distinct colors}"
    local frame
    frame=$(tmux capture-pane -t "$session" -p -e 2>/dev/null)

    # Extract unique ANSI SGR sequences, excluding bare reset (^[[0m)
    local count
    count=$(printf '%s' "$frame" \
        | cat -v \
        | grep -oE '\^\[\[[0-9;]+m' \
        | grep -v '^\^\[\[0m$' \
        | sort -u | wc -l | tr -d ' ')

    if [ "$count" -ge "$min_count" ]; then
        echo "PASS: $label (found $count distinct)"
        return 0
    fi
    echo "FAIL: $label (found $count distinct, need $min_count)"
    return 1
}

# Check that the pane is NOT monochrome (has color beyond default/reset).
# Usage: tmux_assert_not_monochrome <session> [label]
tmux_assert_not_monochrome() {
    local session="$1"
    local label="${2:-pane uses color (not monochrome)}"
    # Need at least 1 non-reset SGR code
    tmux_assert_min_colors "$session" 1 "$label"
}
