# Implicit private Docker service for Linux targets

Depends on: First-class Developer Environment identity and lifecycle

## Purpose

Finish the existing Docker-in-guest wave as an environment-owned default capability of Linux targets rather than a universal Environment requirement or optional global facade.

## Step 1: Re-scope existing Docker foundations

- Move Docker kernel prerequisites from Hardened/Container to Developer.
- Reuse completed artifact provisioning and `EnsureDocker` supervision work from `vz-yr9`.
- Keep dockerd/containerd/BuildKit artifacts checksum-pinned and off the initramfs.
- Prepare and mount an environment-private persistent Docker data root with safe first-use formatting and recovery.

## Step 2: Reconcile and socket-activate services

- Generate environment-specific daemon/containerd configuration.
- Start lazily on first environment Docker API traffic and stream readiness.
- Handle concurrent activation, version mismatch, crashes, stale state, restart, shutdown, and deletion without orphaned processes or mounts.

## Step 3: Enforce youki-only execution

- Configure dockerd's default runtime and containerd shim `BinaryName` to pinned youki.
- Close compatibility gaps in youki rather than adding another runtime.
- Inventory guest-visible and cached artifacts recursively and capture execution-derived runtime evidence.
- Reject runtime override configuration and fail closed when youki cannot be verified.

## Step 4: Scope services per environment

Every Linux-target environment gets independent daemon/containerd processes, sockets, state directories, BuildKit cache, volumes, networks, event streams, configuration, and credentials even if an implementation temporarily shares a VM boundary.

## Validation

- Focused unit/integration tests for provisioning, locking, config, supervisor, activation, and recovery.
- Real VM lifecycle matrix using Docker Engine API through the host bridge.
- Inventory proves no runc/crun or alternate runtime is installed or executed.
- Starting/stopping one environment does not change another's Docker service.
