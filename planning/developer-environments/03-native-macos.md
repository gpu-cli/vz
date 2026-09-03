# Native macOS Machines on macOS

Depends on: First-class Environment topology, Machine identity, and durable lifecycle

## Purpose

Lift the existing macOS VM machinery into the shared Machine lifecycle so native
macOS and Linux Machines can participate in one Environment topology while
preserving native macOS process and service semantics.

## Step 1: Adapt existing macOS VM assets

- Reuse pinned IPSW/base descriptors, provisioning, guest agent, vsock execution, account policy, image verification, patching, save/restore, and Virtualization.framework integration.
- Move these behind the shared HostBackend/TargetAdapter contracts rather than
  exposing a separate `vz vm` product lifecycle.
- Give each macOS Machine an immutable verified base plus private writable disk,
  Machine identity/incarnation, auxiliary storage, unique MAC address, locks,
  network attachments, and Environment ownership record.

## Step 2: Define native workspace behavior

- Mount or synchronize the project/worktree using authorized macOS target paths.
- Provide native shell, streaming exec, PTY/signals, launchd services, networking, ports, persistent state, logs, GUI/headless modes, stop/restart/delete, and capability reporting.
- Do not create an implicit Linux sidecar or advertise Docker. When Docker is
  required, the ProjectDefinition declares a Linux Machine in the topology and
  communication uses declared networks/endpoints.

## Step 3: Reconcile reproducibility

Pin macOS image/build/channel and provisioning inputs. Treat saved state as an optional same-host, exact-configuration acceleration—not the portable source of reproducibility. Make update, retirement, repair, rollback, and destructive replacement explicit.

## Validation

- Contract tests for macOS Machine TargetSpec, capabilities, lifecycle, and
  unsupported Docker errors.
- Real macOS-on-macOS Machine exec/service/network/storage/lifecycle tests plus
  one mixed Linux/macOS Environment topology.
- Legacy `vz vm` records migrate safely; the public command is removed with
  actionable guidance rather than retained as an alias.
