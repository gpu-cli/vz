# Developer Environments: Product Contract

Date: 2026-09-02
Status: committed direction; implementation status is tagged below

This document is the canonical product contract for vz. When older documents use
`sandbox`, `container`, or `VM` as the top-level product object, interpret those
as implementation mechanisms unless that document explicitly describes a
low-level API.

## Product definition

**vz creates reproducible, parallel Developer Environments on local hardware.**

A Developer Environment is the user-facing object. It owns a stable identity, a
project or worktree binding, compute and filesystem state, lifecycle, networking,
tooling, and agent sessions. A sandbox, VM, container, or process boundary is a
backend selected to implement that environment; it is not a competing product
concept the user must assemble.

The environment target is an operating system, independent of the host operating
system. Linux is the universal target: the same Linux Developer Environment
contract is available on macOS today and will extend to Linux and Windows hosts.
Native targets complement that universal target where the host platform permits
them.

## Host and target matrix

Status labels describe backend availability, not complete feature parity:

- **ACTIVE**: working backend capabilities exist in the repository today.
- **DEV**: implementation exists or is actively being unified, but the full
  Developer Environment contract is not complete.
- **PLANNED**: committed direction with no claim of a complete implementation.
- **N/A**: not a supported host/target pairing.

| Host | Linux target | macOS target | Windows target |
|---|---|---|---|
| macOS on Apple silicon | **ACTIVE** — local Linux VM, OCI, BuildKit, and environment primitives; unified lifecycle and full Docker workflow are **DEV** | **ACTIVE** — native macOS VM flows; unified Developer Environment lifecycle is **DEV** | N/A |
| Linux | **DEV** — native Linux backend exists; contract and conformance parity remain in progress | N/A | N/A |
| Windows | **PLANNED** — Linux environment through the appropriate Windows virtualization backend | N/A | **PLANNED** — native Windows environment, delivered after Linux-on-Windows |

Delivery order is therefore:

1. Linux and native macOS targets on macOS.
2. Linux target on Linux.
3. Linux target on Windows.
4. Native Windows target on Windows.

Linux being universal does not mean every host uses the same isolation
mechanism. macOS and Windows host Linux through virtualization; Linux can use
native namespaces, cgroups, and optional VM backends. The observable environment
contract and evidence requirements remain consistent.

## Shared core contract

Every Developer Environment, regardless of target, must provide:

- stable environment identity and deterministic project/worktree association;
- create, start, attach, stop, restart, inspect, and delete lifecycle semantics;
- reproducible inputs and an inspectable description of the realized environment;
- persistent and disposable state with explicit ownership and cleanup;
- streaming exec, stdin, stdout/stderr, PTY, cancellation, and exit status;
- explicit file sharing, networking, port forwarding, credentials, and policy;
- isolation from the host and from every other concurrent environment;
- independent names, sockets, ports, mounts, caches, processes, and credentials;
- agent- and human-facing control surfaces over the same runtime contract;
- release-built, end-to-end verification on the real host/target backend.

Backend-specific capabilities may differ, but differences must be declared and
must not silently weaken isolation or redirect an operation to another
environment.

## Target capabilities

### Linux target

Docker compatibility is an implicit capability of every Linux Developer
Environment. It is not an optional facade and is not a global service. Creating
or starting the environment makes its Docker endpoint available as part of
environment readiness.

Each Linux Developer Environment owns an independent Docker Engine, containerd
instance, image and volume stores, networks, BuildKit cache, persistent data,
host proxy endpoint, and Docker context. Host `docker`, `docker compose`, and
`docker buildx` clients select a specific environment through a vz-managed
context or environment emitted for that environment. vz must never automatically
replace the user's default Docker context or fall back to Docker Desktop, a
global daemon, or a different environment.

There is no global `~/.vz/docker.sock`. Socket paths are private implementation
details resolved from stable environment identity and kept within platform path
limits. Users and integrations address an environment by identity or managed
Docker context rather than constructing a socket pathname.

The Linux target also owns OCI execution, Linux images, Compose workloads,
BuildKit builds, Linux networking, and Linux checkpoint capabilities. On the
current macOS backend, those workloads execute inside the environment's Linux VM.
The committed runtime invariant is that guest container execution uses the
pinned, verified youki runtime without an undeclared runc/crun fallback.

Full host-Docker-CLI parity and its dedicated local-Mac end-to-end gate are
**DEV**, not an ACTIVE claim.

### macOS target

A macOS Developer Environment runs native macOS workloads in a macOS VM and
supports macOS toolchains and behaviors that cannot be represented by a Linux
container. Docker is not implicit because Docker Engine is a Linux-target
capability. A workflow that needs Docker selects or creates a Linux Developer
Environment rather than hiding a shared Linux daemon behind the macOS target.

Existing macOS VM lifecycle, base, validation, and patch capabilities are
**ACTIVE**. Their consolidation behind the shared Developer Environment contract
is **DEV**.

### Windows target

Linux-on-Windows is delivered before native Windows-on-Windows. Linux
environments retain the Linux contract, including per-environment Docker.
Native Windows environments provide Windows workloads and Windows-native tooling;
Docker is not implicit in that target contract. Both Windows pairings are
**PLANNED**.

## User experience contract

The normal workflow names the environment, not its backend:

```text
vz dev create
vz dev start
vz dev exec -- <command>
vz dev stop
```

Exact command spelling may evolve while the unified surface is **DEV**, but these
semantics are fixed:

- environment creation realizes the target's standard capabilities;
- Linux environment readiness includes its private Docker service;
- commands may select an environment explicitly or by an unambiguous project /
  worktree association;
- parallel environments are the default design case;
- ambiguous selection fails closed;
- stopping an environment removes live endpoints but preserves declared
  persistent state;
- deleting an environment removes its managed endpoints and owned state;
- legacy `sandbox`, `container`, `vm`, `run`, and `stack` surfaces remain honest
  descriptions of current mechanisms while they converge on this contract.

## Product boundary

vz is local-first. Hosted placement may reuse the runtime contract later, but is
not required to define the product. The value is reproducible parallel
environments with explicit boundaries and native host/target support—not a claim
that lockdown alone is sufficient.

The locked-down Hardened profile (currently stored and accepted under the legacy
`Container` name during migration) remains a specialized Linux isolation option.
It must not constrain the Developer profile from enabling user namespaces,
cgroups, networking, Docker, or other capabilities required by reproducible
development workflows. Security posture is explicit per profile and target.

## Documentation rule

Product, planning, CLI, and architecture documents should use **Developer
Environment** for the primary user-facing object. Use `sandbox`, `container`,
`VM`, and `process` when naming a compatibility command, protocol entity, security
boundary, or backend implementation. Capability claims must carry **ACTIVE**,
**DEV**, or **PLANNED** status whenever a reader could mistake direction for
shipped behavior.
