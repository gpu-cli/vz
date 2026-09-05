# Runbook: Checkpoint/Cache Portability Failures

## Trigger Conditions

- `export_checkpoint` or `import_checkpoint` fails.
- `export_space_cache` or `import_space_cache` fails.
- btrfs send/receive errors in portability tests or production runs.

## Common Failure Classes

1. Non-btrfs path:
- Symptom: snapshot/send/receive failure under `/tmp` or non-btrfs mount.
- Action: ensure runtime/state/portable paths are under btrfs workspace.

2. Read-only receive semantics:
- Symptom: received subvolume not writable after import.
- Action: verify writable snapshot handoff after receive.

3. Missing stream paths:
- Symptom: `stream_path not found` or parent directory creation errors.
- Action: pre-create parent paths and verify permissions.

## Triage Commands

```bash
findmnt -T "${VZ_TEST_BTRFS_WORKSPACE:-/mnt/vz-btrfs}"
btrfs filesystem usage "${VZ_TEST_BTRFS_WORKSPACE:-/mnt/vz-btrfs}"
```

The former `scripts/run-vz-linux-vm-e2e-hostboot.sh` test generator is
[retired](../retired-cli-workflows.md), including its portability option. It
cannot be used as a current recovery or evidence workflow.

For portability evidence, use the dedicated Linux btrfs tests described in
[the backend guide](../sandbox-vm-e2e.md#linux-btrfs-portability-gate) against an
already established, exactly owned Linux target and real btrfs workspace.
Provisioning that target must not revive the retired VM CLI. If no supported
typed target driver is available, leave this gate incomplete. The supported
local-Mac sandbox command is
`scripts/run-sandbox-vm-e2e.sh --profile release --suite all`; its success is not
a substitute for the send/receive portability assertions below.

## Recovery Steps

1. Move runtime/store/portable directories to btrfs-backed workspace.
2. Re-run failed export/import with same request ids when possible.
3. If receive artifacts are immutable, create writable snapshot and delete received readonly subvolume.

## Validation Exit Criteria

- Portability test suite passes:
  - `spaces_btrfs_checkpoint_restore_and_fork_use_real_subvolumes`
  - `checkpoint_export_import_round_trip_preserves_workspace_snapshot`
  - `space_cache_export_import_round_trip_preserves_payload`
- No active btrfs health degradation alerts.
