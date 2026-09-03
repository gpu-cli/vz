# Implicit private Docker service for each Linux Machine

Depends on: First-class Environment topology, Machine identity, and durable lifecycle

## Purpose

Finish the Docker-in-guest wave as a Machine-owned default capability of every
Linux target rather than an Environment-wide daemon, universal capability, or
optional global facade.

## Step 1: Re-scope existing Docker foundations

- Move Docker kernel prerequisites from Hardened/Container to Developer.
- Reuse completed artifact provisioning and `EnsureDocker` supervision work from `vz-yr9`.
- Keep dockerd/containerd/BuildKit artifacts checksum-pinned and off the initramfs.
- Prepare and mount a private persistent Docker data root for every Linux
  Machine, keyed by Environment and Machine identity.

## Step 2: Reconcile and socket-activate services

- Generate Machine-specific daemon/containerd configuration.
- Start lazily on first Machine Docker API traffic and stream readiness.
- Handle concurrent activation, version mismatch, crashes, stale state, restart, shutdown, and deletion without orphaned processes or mounts.

## Step 3: Enforce youki-only execution

- Configure dockerd's default runtime and containerd shim `BinaryName` to pinned youki.
- Close compatibility gaps in youki rather than adding another runtime.
- Inventory guest-visible and cached artifacts recursively and capture execution-derived runtime evidence.
- Reject runtime override configuration and fail closed when youki cannot be verified.

## Step 4: Scope services per Linux Machine

Every Linux Machine gets independent daemon/containerd processes, sockets, state
directories, BuildKit cache, volumes, networks, event streams, configuration,
credentials, endpoint, and context. Two Linux Machines in one Environment must
not collapse onto a shared engine even if lower-level infrastructure is pooled.

## Validation

- Focused unit/integration tests for provisioning, locking, config, supervisor, activation, and recovery.
- Real VM lifecycle matrix using Docker Engine API through the host bridge.
- Inventory proves no runc/crun or alternate runtime is installed or executed.
- Starting/stopping one Machine does not change a sibling or another
  Environment's Docker service.
- Two Linux Machines in one Environment pass independent Engine/state/context
  tests from the host Docker CLI.
