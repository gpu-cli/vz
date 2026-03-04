#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run-runtime-soak.sh --workload-cmd "<cmd>" --daemon-pid <pid> [options]

Required:
  --workload-cmd "<cmd>"         Command executed each iteration.
  --daemon-pid <pid>             Runtime daemon PID to sample.

Options:
  --iterations <n>               Iteration count (default: 120)
  --max-rss-growth-kb <n>        Max allowed RSS growth from baseline (default: 131072)
  --max-fd-growth <n>            Max allowed FD growth from baseline (default: 256)
  --sleep-secs <n>               Delay between iterations (default: 1)
  --report <path>                Output JSON report path (default: .artifacts/runtime-soak/latest.json)

Notes:
  - Linux-only: reads /proc/<pid>/status and /proc/<pid>/fd.
  - Exits non-zero when thresholds are exceeded.
EOF
}

ITERATIONS=120
MAX_RSS_GROWTH_KB=131072
MAX_FD_GROWTH=256
SLEEP_SECS=1
REPORT_PATH=".artifacts/runtime-soak/latest.json"
WORKLOAD_CMD=""
DAEMON_PID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workload-cmd) WORKLOAD_CMD="${2:-}"; shift 2 ;;
    --daemon-pid) DAEMON_PID="${2:-}"; shift 2 ;;
    --iterations) ITERATIONS="${2:-}"; shift 2 ;;
    --max-rss-growth-kb) MAX_RSS_GROWTH_KB="${2:-}"; shift 2 ;;
    --max-fd-growth) MAX_FD_GROWTH="${2:-}"; shift 2 ;;
    --sleep-secs) SLEEP_SECS="${2:-}"; shift 2 ;;
    --report) REPORT_PATH="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$WORKLOAD_CMD" || -z "$DAEMON_PID" ]]; then
  usage
  exit 2
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "error: run-runtime-soak.sh requires Linux (/proc sampling)" >&2
  exit 2
fi

if [[ ! -d "/proc/$DAEMON_PID" ]]; then
  echo "error: daemon pid $DAEMON_PID not found under /proc" >&2
  exit 2
fi

read_rss_kb() {
  local pid="$1"
  awk '/VmRSS:/ {print $2}' "/proc/$pid/status"
}

read_fd_count() {
  local pid="$1"
  ls -1 "/proc/$pid/fd" | wc -l | tr -d ' '
}

BASELINE_RSS_KB="$(read_rss_kb "$DAEMON_PID")"
BASELINE_FD_COUNT="$(read_fd_count "$DAEMON_PID")"
MAX_OBSERVED_RSS_KB="$BASELINE_RSS_KB"
MAX_OBSERVED_FD_COUNT="$BASELINE_FD_COUNT"
FAILED=0
FAIL_REASON=""

START_UNIX="$(date +%s)"

for ((i=1; i<=ITERATIONS; i++)); do
  if [[ ! -d "/proc/$DAEMON_PID" ]]; then
    FAILED=1
    FAIL_REASON="daemon_pid_exited"
    break
  fi

  bash -lc "$WORKLOAD_CMD"

  RSS_KB="$(read_rss_kb "$DAEMON_PID")"
  FD_COUNT="$(read_fd_count "$DAEMON_PID")"
  if (( RSS_KB > MAX_OBSERVED_RSS_KB )); then
    MAX_OBSERVED_RSS_KB="$RSS_KB"
  fi
  if (( FD_COUNT > MAX_OBSERVED_FD_COUNT )); then
    MAX_OBSERVED_FD_COUNT="$FD_COUNT"
  fi

  RSS_GROWTH_KB=$(( RSS_KB - BASELINE_RSS_KB ))
  FD_GROWTH=$(( FD_COUNT - BASELINE_FD_COUNT ))

  if (( RSS_GROWTH_KB > MAX_RSS_GROWTH_KB )); then
    FAILED=1
    FAIL_REASON="rss_growth_exceeded"
    break
  fi
  if (( FD_GROWTH > MAX_FD_GROWTH )); then
    FAILED=1
    FAIL_REASON="fd_growth_exceeded"
    break
  fi

  sleep "$SLEEP_SECS"
done

END_UNIX="$(date +%s)"
MAX_RSS_GROWTH_KB_OBS=$(( MAX_OBSERVED_RSS_KB - BASELINE_RSS_KB ))
MAX_FD_GROWTH_OBS=$(( MAX_OBSERVED_FD_COUNT - BASELINE_FD_COUNT ))

mkdir -p "$(dirname "$REPORT_PATH")"
cat >"$REPORT_PATH" <<EOF
{
  "daemon_pid": $DAEMON_PID,
  "iterations": $ITERATIONS,
  "workload_cmd": $(printf '%s' "$WORKLOAD_CMD" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),
  "start_unix": $START_UNIX,
  "end_unix": $END_UNIX,
  "baseline_rss_kb": $BASELINE_RSS_KB,
  "baseline_fd_count": $BASELINE_FD_COUNT,
  "max_observed_rss_kb": $MAX_OBSERVED_RSS_KB,
  "max_observed_fd_count": $MAX_OBSERVED_FD_COUNT,
  "max_observed_rss_growth_kb": $MAX_RSS_GROWTH_KB_OBS,
  "max_observed_fd_growth": $MAX_FD_GROWTH_OBS,
  "threshold_rss_growth_kb": $MAX_RSS_GROWTH_KB,
  "threshold_fd_growth": $MAX_FD_GROWTH,
  "passed": $( [[ $FAILED -eq 0 ]] && echo "true" || echo "false" ),
  "fail_reason": $(printf '%s' "${FAIL_REASON:-}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
}
EOF

if [[ $FAILED -eq 1 ]]; then
  echo "runtime soak failed: $FAIL_REASON (report: $REPORT_PATH)" >&2
  exit 1
fi

echo "runtime soak passed (report: $REPORT_PATH)"
