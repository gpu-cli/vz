# Native macOS-on-macOS Developer Environments

Depends on: First-class Developer Environment identity and lifecycle

## Purpose

Lift the existing macOS VM machinery into the same first-class Developer Environment lifecycle now, alongside Linux-on-macOS, while preserving native macOS process and service semantics.

## Step 1: Adapt existing macOS VM assets

- Reuse pinned IPSW/base descriptors, provisioning, guest agent, vsock execution, account policy, image verification, patching, save/restore, and Virtualization.framework integration.
- Move these behind the shared HostBackend/TargetAdapter contracts rather than exposing a separate `vz vm` product lifecycle.
- Give each environment an immutable verified base plus private writable disk, machine identity, auxiliary storage, MAC address, locks, and ownership record.

## Step 2: Define native workspace behavior

- Mount or synchronize the project/worktree using authorized macOS target paths.
- Provide native shell, streaming exec, PTY/signals, launchd services, networking, ports, persistent state, logs, GUI/headless modes, stop/restart/delete, and capability reporting.
- Do not create a Linux sidecar or advertise Docker unless a future explicit composed-environment design adds one.

## Step 3: Reconcile reproducibility

Pin macOS image/build/channel and provisioning inputs. Treat saved state as an optional same-host, exact-configuration acceleration—not the portable source of reproducibility. Make update, retirement, repair, rollback, and destructive replacement explicit.

## Validation

- Contract tests for macOS TargetSpec, capabilities, lifecycle, and unsupported Docker errors.
- Real macOS-on-macOS create/shell/exec/service/network/storage/stop/restart/delete tests.
- Existing `vz vm` flows remain compatible aliases during migration.
