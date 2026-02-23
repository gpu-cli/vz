#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CRATE_ROOT="$REPO_ROOT/crates"
VZ_BIN="$CRATE_ROOT/target/debug/vz"

if [[ ! -x "$VZ_BIN" ]]; then
  (cd "$CRATE_ROOT" && cargo build -p vz-cli)
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

TOTAL_PASS_CASES=60
PASS_COUNT=0
EXPECTED_FAIL_COUNT=0
UNEXPECTED=0
TOTAL=0

declare -a IMAGES=(
  "alpine:latest"
  "python:3.12-alpine"
  "redis:7-alpine"
  "nginx:1.25-alpine"
)

declare -a COMMAND_MODES=(
  "string"
  "array"
)

declare -a ENV_MODES=(
  "none"
  "map"
  "list"
)

declare -a PORT_MODES=(
  "none"
  "short"
  "long"
)

declare -a TOPOLOGIES=(
  "single"
  "linear"
  "fanout"
)

declare -a VOLUME_MODES=(
  "none"
  "bind"
  "named"
)

declare -a RESTART_MODES=(
  "none"
  "always"
  "on-failure"
)

render_command_block() {
  local service_prefix="    command: "
  local mode="$1"
  if [[ "$mode" == "array" ]]; then
    printf '%s%s%s\n' "$service_prefix" '["sleep", "120"]'
  else
    printf '%s%s\n' "$service_prefix" '"sleep 120"'
  fi
}

render_env_block() {
  local mode="$1"
  local case_id="$2"
  local service_name="$3"

  if [[ "$mode" == "map" ]]; then
    printf '    environment:\n'
    printf '      MODE: map\n'
    printf '      SERVICE: %s\n' "$service_name"
    printf '      CASE_ID: "%s"\n' "$case_id"
  elif [[ "$mode" == "list" ]]; then
    printf '    environment:\n'
    printf '      - MODE=list\n'
    printf '      - SERVICE=%s\n' "$service_name"
    printf '      - CASE_ID=%s\n' "$case_id"
  fi
}

render_ports_block() {
  local mode="$1"
  local host_port="$2"

  if [[ "$mode" == "short" ]]; then
    printf '    ports:\n'
    printf '      - "%s:80"\n' "$host_port"
  elif [[ "$mode" == "long" ]]; then
    printf '    ports:\n'
    printf '      - target: 80\n'
    printf '        published: %s\n' "$host_port"
    printf '        protocol: tcp\n'
  fi
}

render_restart_block() {
  local mode="$1"
  if [[ "$mode" == "none" ]]; then
    return
  fi
  printf '    restart: %s\n' "$mode"
}

render_volume_block() {
  local mode="$1"
  local case_id="$2"
  local suffix="$3"

  if [[ "$mode" == "bind" ]]; then
    printf '    volumes:\n'
    printf '      - /tmp:%s\n' "/tmp/$suffix"
  elif [[ "$mode" == "named" ]]; then
    printf '    volumes:\n'
    printf '      - shared_data_%s:%s\n' "$case_id" "/shared/$suffix"
  fi
}

render_network_block() {
  local enabled="$1"
  if [[ "$enabled" == "1" ]]; then
    printf '    networks:\n'
    printf '      - stack_net\n'
  fi
}

render_service_block() {
  local name="$1"
  local image="$2"
  local command_mode="$3"
  local env_mode="$4"
  local port_mode="$5"
  local restart_mode="$6"
  local volume_mode="$7"
  local use_network="$8"
  local depends_on="$9"
  local host_port="${10}"
  local case_id="$11"
  local suffix="$12"

  printf '  %s:\n' "$name"
  printf '    image: %s\n' "$image"
  render_command_block "$command_mode"
  render_env_block "$env_mode" "$case_id" "$name"
  if [[ -n "$depends_on" ]]; then
    printf '    depends_on:\n'
    printf '      - %s\n' "$depends_on"
  fi
  render_ports_block "$port_mode" "$host_port"
  render_restart_block "$restart_mode"
  render_volume_block "$volume_mode" "$case_id" "$suffix"
  render_network_block "$use_network"
}

build_case_file() {
  local case_id="$1"
  local compose_file="$2"
  local image="$3"
  local command_mode="$4"
  local env_mode="$5"
  local port_mode="$6"
  local topology="$7"
  local volume_mode="$8"
  local restart_mode="$9"

  local host_port=$((18000 + case_id))
  local use_network=0
  if (( case_id % 4 == 0 )); then
    use_network=1
  fi

  local xvz_line=""
  if (( case_id % 5 == 0 )); then
    xvz_line=$'x-vz:\n  disk_size: 256m\n'
  fi

  local top_volumes=""
  local top_network=""
  if [[ "$volume_mode" == "named" ]]; then
    top_volumes=$'volumes:\n  shared_data_'"$case_id"$': {}\n'
  fi
  if (( use_network == 1 )); then
    top_network=$'networks:\n  stack_net: {}\n'
  fi

  {
    printf 'version: "3.9"\n'
    printf '%s' "$xvz_line"
    printf '%s' "$top_network"
    printf '%s' "$top_volumes"
    printf 'services:\n'

    if [[ "$topology" == "single" ]]; then
      render_service_block "app" "$image" "$command_mode" "$env_mode" "$port_mode" "$restart_mode" "$volume_mode" "$use_network" "" "$host_port" "$case_id" "single"
    elif [[ "$topology" == "linear" ]]; then
      render_service_block "db" "redis:7-alpine" "$command_mode" "none" "none" "none" "$volume_mode" "$use_network" "" "$((host_port + 1))" "$case_id" "db"
      render_service_block "app" "$image" "$command_mode" "$env_mode" "$port_mode" "$restart_mode" "$volume_mode" "$use_network" "db" "$host_port" "$case_id" "app"
    else
      render_service_block "db" "redis:7-alpine" "$command_mode" "none" "none" "none" "$volume_mode" "$use_network" "" "$((host_port + 1))" "$case_id" "db"
      render_service_block "api" "$image" "$command_mode" "$env_mode" "$port_mode" "$restart_mode" "$volume_mode" "$use_network" "db" "$host_port" "$case_id" "api"
      render_service_block "worker" "$image" "$command_mode" "$env_mode" "none" "$restart_mode" "$volume_mode" "$use_network" "db" "$((host_port + 2))" "$case_id" "worker"
    fi
  } > "$compose_file"
}

run_case() {
  local case_id="$1"
  local expected="$2"
  local compose_file="$3"

  local observed output
  if output=$("$VZ_BIN" stack config -f "$compose_file" --quiet 2>&1); then
    observed="pass"
  else
    observed="fail"
  fi

  TOTAL=$((TOTAL + 1))
  if [[ "$observed" == "$expected" ]]; then
    if [[ "$expected" == "pass" ]]; then
      PASS_COUNT=$((PASS_COUNT + 1))
    else
      EXPECTED_FAIL_COUNT=$((EXPECTED_FAIL_COUNT + 1))
    fi
    printf '%-40s PASS (%s)\n' "$case_id" "$observed"
    return
  fi

  UNEXPECTED=$((UNEXPECTED + 1))
  printf '%-40s EXPECTED %s GOT %s\n' "$case_id" "$expected" "$observed" >&2
  printf '  compose: %s\n' "$compose_file" >&2
  printf '  output: %s\n' "${output}" >&2
}

for case_id in $(seq 0 $((TOTAL_PASS_CASES - 1))); do
  selector="$case_id"
  image=${IMAGES[$((selector % ${#IMAGES[@]}))]}
  selector=$((selector / ${#IMAGES[@]}))

  command_mode=${COMMAND_MODES[$((selector % ${#COMMAND_MODES[@]}))]}
  selector=$((selector / ${#COMMAND_MODES[@]}))

  env_mode=${ENV_MODES[$((selector % ${#ENV_MODES[@]}))]}
  selector=$((selector / ${#ENV_MODES[@]}))

  port_mode=${PORT_MODES[$((selector % ${#PORT_MODES[@]}))]}
  selector=$((selector / ${#PORT_MODES[@]}))

  topology=${TOPOLOGIES[$((selector % ${#TOPOLOGIES[@]}))]}
  selector=$((selector / ${#TOPOLOGIES[@]}))

  volume_mode=${VOLUME_MODES[$((selector % ${#VOLUME_MODES[@]}))]}
  selector=$((selector / ${#VOLUME_MODES[@]}))

  restart_mode=${RESTART_MODES[$((selector % ${#RESTART_MODES[@]}))]}

  compose_file="$WORK_DIR/pass-$case_id.yaml"
  build_case_file "$case_id" "$compose_file" "$image" "$command_mode" "$env_mode" "$port_mode" "$topology" "$volume_mode" "$restart_mode"
  run_case "pass-${case_id}" pass "$compose_file"
done

TOTAL_EXPECTED_FAIL=10
fail_dir="$WORK_DIR/fail"
mkdir -p "$fail_dir"

cat > "$fail_dir/unsupported-key.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    build: .
    command: sleep 60
EOF
run_case unsupported-key fail "$fail_dir/unsupported-key.yaml"

cat > "$fail_dir/missing-dependency.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    command: sleep 60
    depends_on:
      - db
EOF
run_case missing-dependency fail "$fail_dir/missing-dependency.yaml"

cat > "$fail_dir/invalid-port.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    command: sleep 60
    ports:
      - "bad-port"
EOF
run_case invalid-port fail "$fail_dir/invalid-port.yaml"

cat > "$fail_dir/bad-restart.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    command: sleep 60
    restart: maybe
EOF
run_case bad-restart fail "$fail_dir/bad-restart.yaml"

cat > "$fail_dir/invalid-secret.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    command: sleep 60
    secrets:
      - bad
secrets:
  bad:
    external: true
EOF
run_case invalid-secret fail "$fail_dir/invalid-secret.yaml"

cat > "$fail_dir/missing-volume.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    command: sleep 60
    volumes:
      - not-defined:/data
EOF
run_case missing-volume fail "$fail_dir/missing-volume.yaml"

cat > "$fail_dir/missing-image.yaml" <<'EOF'
version: "3.9"
services:
  app:
    command: sleep 60
EOF
run_case missing-image fail "$fail_dir/missing-image.yaml"

cat > "$fail_dir/invalid-deploy.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    command: sleep 60
    deploy:
      resources:
        limits:
          cpus: "bad"
EOF
run_case invalid-deploy fail "$fail_dir/invalid-deploy.yaml"

cat > "$fail_dir/bad-env-mode.yaml" <<'EOF'
version: "3.9"
services:
  app:
    image: alpine:latest
    command: sleep 60
    environment:
      -
EOF
run_case bad-env-mode fail "$fail_dir/bad-env-mode.yaml"

cat > "$fail_dir/invalid-top-level.yaml" <<'EOF'
version: "3.9"
configs:
  app-config:
    file: ./config.txt
services:
  app:
    image: alpine:latest
EOF
run_case invalid-top-level fail "$fail_dir/invalid-top-level.yaml"

echo "Summary: $PASS_COUNT/$TOTAL_PASS_CASES pass cases and $EXPECTED_FAIL_COUNT/$TOTAL_EXPECTED_FAIL expected failures"
echo "Total evaluated: $TOTAL | unexpected outcomes: $UNEXPECTED"

if [[ "$UNEXPECTED" -ne 0 ]]; then
  echo "ERROR: matrix produced unexpected outcomes. See output above." >&2
  exit 1
fi

if [[ "$PASS_COUNT" -ne "$TOTAL_PASS_CASES" ]]; then
  echo "WARNING: not all pass cases succeeded as expected." >&2
  exit 1
fi

echo "PASS: Linux stack compose config matrix completed."
exit 0
