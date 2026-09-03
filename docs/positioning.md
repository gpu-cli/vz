# vz Positioning — Developer Environments first

Date: 2026-09-02
Status: committed direction; this is not a shipped-features list
Audience: contributors, collaborators, and product evaluators

The canonical contract is
[`developer-environments.md`](developer-environments.md). This brief explains
the product decision; the contract controls when wording differs.

## The one-liner

**vz creates reproducible, parallel project topologies on local hardware: many
isolated Developer Environment instances, each containing one or more
target-native Machines. Linux is universal; native Machines complement it where
the host permits them.**

## The decision

The Developer Environment is the primary user-facing isolation, ownership, and
lifecycle object. A project definition may create many instances, including
several for one worktree. Each contains target-native Machines and owns their
storage, network fabric, endpoints, policy, and evidence. Target OS belongs to
each Machine. `sandbox`, `container`, `VM`, and `process` name boundaries and
backends, not separate products.

This corrects the earlier emphasis on lockdown as the product. Strong boundaries
remain necessary, but the differentiating value is repeatability and safe
parallelism: many isolated topology instances and many cooperating Machines on
powerful local hardware.

## Linux everywhere, native where it matters

Linux is the universal Machine target:

- **ACTIVE on macOS:** Linux VM, OCI, BuildKit, and environment primitives exist;
  the unified experience and full Docker workflow are **DEV**.
- **DEV on Linux:** a native backend exists and is converging on the same
  contract.
- **PLANNED on Windows:** Linux-on-Windows follows the established Linux target
  contract.

Native targets complement Linux rather than fragmenting its contract:

- **ACTIVE on macOS:** native macOS VM flows exist; their unified Developer
  Environment surface is **DEV**.
- **PLANNED on Windows:** native Windows-on-Windows follows Linux-on-Windows.

The backend is intentionally platform-specific. Apple Virtualization.framework,
Linux namespaces/cgroups or VM backends, and the future Windows virtualization
backend can implement one environment lifecycle without pretending their kernels
or capabilities are identical.

## Docker is part of every Developer-profile Linux Machine

Full Docker compatibility is not an optional facade. It is an implicit
capability of every Linux Developer Machine. Creating or starting one makes its
Docker service available as part of Machine readiness.

Every Developer-profile Linux Machine owns its own Docker Engine, containerd, image and volume
stores, networks, BuildKit cache, persistent data, proxy endpoint, and Docker
context. The host's unmodified `docker`, Compose, and buildx clients select that
specific Environment/Machine. There is no Environment-global or global
`~/.vz/docker.sock`, no global vz Docker daemon, and no fallback to Docker
Desktop, a sibling Machine, or a neighboring Environment.

Docker is not implicit for native macOS or native Windows targets. A workflow
requiring Docker declares a Linux Machine, preserving an honest OS boundary
rather than hiding a shared Linux service inside a native Machine.

The current youki substrate remains central to the Linux implementation. The
committed runtime invariant is one pinned, verified OCI runtime in the guest,
with Docker/containerd and BuildKit invoking youki and no undeclared runc/crun
fallback. Full host-Docker-CLI parity remains **DEV** until the dedicated real-Mac
end-to-end contract passes.

## The durable product advantage

vz exposes the Environment topology instead of hiding it. Project, Environment,
and Machine identity, targets, configuration, state, declared routes, and
evidence are inspectable. The same contract serves humans, agents, and automation.

Parallelism has two axes: several isolated Environments per project/worktree and
several cooperating Machines per Environment. Separate Environments may share
immutable inputs but not mutable state, routing, DNS, sockets, ports, mounts,
processes, images, volumes, caches, credentials, or identities. Machines use
declared private or simulated-public paths; ambiguous selection fails closed.

This model scales with local hardware: more CPU and memory translate directly
into more concurrent reproducible environments without turning a single global
daemon into the coordination boundary.

## Architecture boundary

On macOS, each Developer-profile Linux Machine—including its Docker daemon—uses its selected Linux
backend, while each native macOS Machine uses a macOS VM. They may participate
in one Environment through its declared network fabric.
On Linux, the native backend uses Linux isolation primitives and may offer VM
placement under the same contract. Future Windows backends provide Linux and
native Windows placement explicitly.

The host runs the control plane and declared transport, filesystem, networking,
and credential bridges. Host effects are allowed only through those visible
channels. The architecture must not make a universal claim that every backend is
a Linux VM or that nothing executes natively on any host.

## Status and roadmap

### ACTIVE

- Linux VM/container/BuildKit foundations on Apple-silicon macOS.
- Native macOS VM flows.
- Current `run`, `stack`, `build`, `docker`, `sandbox`, and `vm` mechanisms.
- Runtime daemon/API and checkpoint foundations documented elsewhere in the repo.

### DEV

- A Project/Environment/Machine topology identity and lifecycle.
- Implicit, per-Developer-Linux-Machine Docker, driven end to end by the local Mac's
  Docker/Compose/buildx clients.
- Private/public-like topology networking, per-Machine endpoints, persistence,
  isolation, recovery, and comprehensive real-VM evidence.
- Linux-host conformance with the Linux target contract.
- Agent data-plane and filesystem surfaces required for a complete environment.

### PLANNED

- Linux Machines and Environment topologies on Windows.
- Native Windows Machines in those topologies on Windows.
- Additional placement, collaboration, policy, and hosted capabilities that
  preserve the same explicit target and identity model.

## How we talk about vz

- Say **Developer Environment** for the product object.
- Say **Machine** for a target-native compute member and qualify capabilities at
  that level.
- Say sandbox, container, VM, process, youki, Docker, or BuildKit when explaining
  a security boundary, backend, protocol, or current compatibility command.
- Say Linux is the universal Machine target, not that vz is Linux-only.
- Say Docker is implicit for Developer-profile Linux Machines, not a global socket or optional
  facade.
- State the host and target when describing a capability.
- Tag capabilities **ACTIVE**, **DEV**, or **PLANNED** and require real
  host×Machine-target end-to-end evidence before declaring parity.
- Lead with reproducibility and parallel environments; describe lockdown as one
  selectable security posture, not the entire reason the product exists.
