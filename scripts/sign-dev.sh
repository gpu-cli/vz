#!/usr/bin/env bash
# Ad-hoc code signing for local development.
#
# Signs vz-cli and vz-guest-agent with the required entitlements so that
# Virtualization.framework allows VM operations on the developer's own machine.
#
# Usage:
#   ./scripts/sign-dev.sh                    # Sign release builds
#   ./scripts/sign-dev.sh --profile debug    # Sign debug builds
#
# This script uses ad-hoc signing (--sign -), which works on the local machine
# but will trigger Gatekeeper warnings on other machines. For distribution,
# use the CI release workflow with a Developer ID certificate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENTITLEMENTS="$REPO_ROOT/entitlements/vz-cli.entitlements.plist"

PROFILE="release"
if [[ "${1:-}" == "--profile" ]] && [[ -n "${2:-}" ]]; then
    PROFILE="$2"
fi

TARGET_DIR="$REPO_ROOT/crates/target/$PROFILE"

if [[ ! -f "$ENTITLEMENTS" ]]; then
    echo "error: entitlements plist not found at $ENTITLEMENTS" >&2
    exit 1
fi

sign_binary() {
    local binary="$1"
    local entitlements="${2:-}"

    if [[ ! -f "$binary" ]]; then
        echo "skip: $binary (not found, build it first)" >&2
        return 0
    fi

    local sign_args=(
        --sign -
        --force
    )

    if [[ -n "$entitlements" ]]; then
        sign_args+=(--entitlements "$entitlements")
    fi

    echo "signing: $binary"
    codesign "${sign_args[@]}" "$binary"
    codesign --verify --verbose "$binary"
}

# Sign vz-cli (needs virtualization entitlement)
sign_binary "$TARGET_DIR/vz" "$ENTITLEMENTS"

# Sign vz-guest-agent (no entitlements needed, runs inside the VM)
sign_binary "$TARGET_DIR/vz-guest-agent"

echo "done: all binaries signed (ad-hoc)"
