#!/usr/bin/env bash
set -euo pipefail

# Validate checked-in build sweep context mapping manifest resolution.
# Intended for CI or preflight checks.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST_PATH="${ROOT_DIR}/config/build-sweep-manifest.json"

if [[ ! -f "${MANIFEST_PATH}" ]]; then
  echo "error: missing manifest at ${MANIFEST_PATH}" >&2
  exit 1
fi

if [[ "${OSTYPE:-}" != darwin* ]]; then
  echo "skip: build sweep dry-run check currently runs via macOS-only CLI surface"
  exit 0
fi

echo "==> Running build sweep dry-run mapping check"
(
  cd "${ROOT_DIR}/crates"
  cargo run -p vz-cli -- vm mac validate sweep-build \
    --manifest "${MANIFEST_PATH}" \
    --repo-root "${ROOT_DIR}" \
    --dry-run \
    --json >/dev/null
)
echo "ok: build sweep mapping manifest resolved successfully"
