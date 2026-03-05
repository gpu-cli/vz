#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

proto_file="crates/vz-runtime-proto/proto/runtime_v2.proto"

coverage_files=(
  "crates/vz-runtimed-client/src/lib.rs"
  "crates/vz-runtimed-client/src/linux_vm.rs"
  "crates/vz-runtimed-client/src/tests.rs"
  "crates/vz-runtimed/src/grpc/tests.rs"
  "crates/vz-api/src/tests.rs"
  "crates/vz-api/tests/server_smoke.rs"
)

failures=0
for file in "${coverage_files[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "runtime-v2 rpc coverage gate: missing coverage file: $file" >&2
    failures=$((failures + 1))
  fi
done

if [[ "$failures" -gt 0 ]]; then
  exit 1
fi

tmp_rpcs="$(mktemp)"
trap 'rm -f "$tmp_rpcs"' EXIT

awk '/^[[:space:]]*rpc[[:space:]]+/ {name=$2; sub(/\(.*/, "", name); print name}' \
  "$proto_file" > "$tmp_rpcs"

rpc_total=0
covered=0
missing=()

while IFS= read -r rpc; do
  if [[ -z "$rpc" ]]; then
    continue
  fi
  rpc_total=$((rpc_total + 1))
  snake="$(printf '%s' "$rpc" | sed -E 's/([A-Z])/_\1/g' | tr '[:upper:]' '[:lower:]' | sed 's/^_//')"
  if rg -q "\.${snake}\(" "${coverage_files[@]}"; then
    covered=$((covered + 1))
  else
    missing+=("$rpc")
  fi
done < "$tmp_rpcs"

if [[ "${#missing[@]}" -gt 0 ]]; then
  echo "runtime-v2 rpc coverage gate failed: ${#missing[@]} missing RPC test invocation(s)" >&2
  echo "checked files:" >&2
  for file in "${coverage_files[@]}"; do
    echo "  - $file" >&2
  done
  echo "missing RPCs:" >&2
  for rpc in "${missing[@]}"; do
    echo "  - $rpc" >&2
  done
  exit 1
fi

echo "runtime-v2 rpc coverage gate passed (${covered}/${rpc_total} RPCs)"
