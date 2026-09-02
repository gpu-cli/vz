# vz Positioning — Developer Environments first

Date: 2026-09-02
Status: committed direction; this is not a shipped-features list
Audience: contributors, collaborators, and product evaluators

The canonical contract is
[`developer-environments.md`](developer-environments.md). This brief explains
the product decision; the contract controls when wording differs.

## The one-liner

**vz creates reproducible, parallel Developer Environments on local hardware,
with Linux as the universal target and native environments where the host
platform supports them.**

## The decision

The Developer Environment is the primary user-facing object. It owns the project
or worktree association, target OS, lifecycle, files, compute, network, persistent
state, tooling, and agent sessions. `sandbox`, `container`, `VM`, and `process`
name boundaries and backends, not separate products a developer must compose.

This corrects the earlier emphasis on lockdown as the product. Strong boundaries
remain necessary, but the differentiating value is repeatability and safe
parallelism: many environments on powerful local hardware, each independently
addressable and reproducible.

## Linux everywhere, native where it matters

Linux is the universal Developer Environment target:

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

## Docker is part of a Linux environment

Full Docker compatibility is not an optional facade. It is an implicit
capability of every Linux Developer Environment. Creating or starting one makes
its Docker service available as part of readiness.

Every Linux environment owns its own Docker Engine, containerd, image and volume
stores, networks, BuildKit cache, persistent data, proxy endpoint, and Docker
context. The host's unmodified `docker`, Compose, and buildx clients select that
specific environment. There is no global `~/.vz/docker.sock`, no global vz Docker
daemon, and no fallback to Docker Desktop or a neighboring environment.

Docker is not implicit for native macOS or native Windows targets. A workflow
requiring Docker chooses a Linux target, preserving an honest OS boundary rather
than hiding a shared Linux service inside a native environment.

The current youki substrate remains central to the Linux implementation. The
committed runtime invariant is one pinned, verified OCI runtime in the guest,
with Docker/containerd and BuildKit invoking youki and no undeclared runc/crun
fallback. Full host-Docker-CLI parity remains **DEV** until the dedicated real-Mac
end-to-end contract passes.

## The durable product advantage

vz exposes the environment instead of hiding it. Identity, target, configuration,
state, declared host channels, and verification evidence are inspectable. The
same environment contract serves humans, coding agents, and automation.

Parallelism is a first-order invariant. Two environments may share immutable,
verified inputs, but they do not share mutable daemon state, sockets, ports,
mounts, processes, networks, images, volumes, caches, credentials, or identities.
An ambiguous request fails closed.

This model scales with local hardware: more CPU and memory translate directly
into more concurrent reproducible environments without turning a single global
daemon into the coordination boundary.

## Architecture boundary

On macOS, Linux workloads—including their Docker daemon—execute inside the
selected Linux VM. Native macOS workloads execute inside the selected macOS VM.
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

- A unified Developer Environment identity and lifecycle across current
  mechanisms.
- Implicit, per-environment Docker on Linux, driven end to end by the local Mac's
  Docker/Compose/buildx clients.
- Per-environment Docker proxying, networking, persistence, isolation, recovery,
  and comprehensive real-VM evidence.
- Linux-host conformance with the Linux target contract.
- Agent data-plane and filesystem surfaces required for a complete environment.

### PLANNED

- Linux Developer Environments on Windows.
- Native Windows Developer Environments on Windows.
- Additional placement, collaboration, policy, and hosted capabilities that
  preserve the same explicit target and identity model.

## How we talk about vz

- Say **Developer Environment** for the product object.
- Say sandbox, container, VM, process, youki, Docker, or BuildKit when explaining
  a security boundary, backend, protocol, or current compatibility command.
- Say Linux is the universal target, not that vz is a Linux-only product.
- Say Docker is implicit for Linux targets, not a global socket or optional
  facade.
- State the host and target when describing a capability.
- Tag capabilities **ACTIVE**, **DEV**, or **PLANNED** and require real
  host/target end-to-end evidence before declaring parity.
- Lead with reproducibility and parallel environments; describe lockdown as one
  selectable security posture, not the entire reason the product exists.
