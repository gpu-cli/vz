#!/bin/bash
# Retired entry point. Keep this rejection self-contained and side-effect free.
# The prior implementation remains recoverable in Git history at 98d870c0.
# Do not delegate, discover state, build, boot, or invoke a fallback vz binary.
printf '%s\n' '{"error":{"code":"legacy_workflow_removed","workflow":"run-vz-linux-vm-e2e.sh","message":"This pre-0.4 helper depended on retired CLI commands and is no longer executable.","migration":"Use the topology-scoped typed APIs and the installed Developer Environment lifecycle as implemented; consult vz --help.","backend_verification":"scripts/run-sandbox-vm-e2e.sh --suite all --profile release provides local-Mac backend evidence only, not an equivalent workflow or the complete 0.4 release gate."}}' >&2
exit 2
