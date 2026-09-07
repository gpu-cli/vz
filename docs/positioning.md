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

- <!-- capability-matrix: macos-arm64/linux/* pair -->**DEV on macOS:** Linux Machines on Apple silicon run
  through Virtualization.framework with installed local-Mac evidence; the private
  Docker workflow for Developer Machines is
  <!-- capability-matrix: macos-arm64/linux/developer docker_engine,compose,buildx -->**DEV** and topology networking
  is <!-- capability-matrix: macos-arm64/linux/* network_private -->**PLANNED**.
- <!-- capability-matrix: linux-*/linux/* pair -->**PLANNED on Linux:** a partial native backend
  exists in the tree, but no Machine target resolves on a Linux host yet.
- <!-- capability-matrix: windows-*/linux/* pair -->**PLANNED on Windows:** Linux-on-Windows follows
  the established Linux target contract.

Native targets complement Linux rather than fragmenting its contract:

- <!-- capability-matrix: macos-arm64/macos/developer pair -->**DEV on macOS:** native macOS Developer Machines pass
  installed Up/exec/PTY/Stop/Delete locally; no published release exists.
- <!-- capability-matrix: windows-*/windows/developer pair -->**PLANNED on Windows:** native
  Windows-on-Windows follows Linux-on-Windows.

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
fallback. Full host-Docker-CLI parity remains
<!-- capability-matrix: macos-arm64/linux/developer docker_engine,compose,buildx,docker_context -->**DEV** until the
dedicated real-Mac end-to-end contract passes.

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

Labels follow the `status_definitions` of
[`config/host-target-capabilities-v0.4.json`](../config/host-target-capabilities-v0.4.json);
nothing is labelled shipped until a 0.4 release is published.

- <!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV** — Project/Environment/Machine identity and the
  five-verb lifecycle for Linux and native macOS Machines on Apple-silicon
  macOS, with receipts, demonstrated by installed local-Mac slices.
- <!-- capability-matrix: macos-arm64/linux/developer docker_engine,compose,buildx,docker_context,image_roundtrip,registry_login_pull_push,container_lifecycle_io,youki_only_runtime -->**DEV**
  — implicit, per-Developer-Linux-Machine Docker driven end to end by the local
  Mac's Docker/Compose/buildx clients: private Engine, managed context, image
  round trip, registry login/pull/push, container lifecycle I/O, youki-only
  runtime.
- <!-- capability-matrix: macos-arm64/macos/developer posix_pty -->**DEV** — interactive PTY execution on native macOS
  Machines.
- <!-- capability-matrix: macos-arm64/linux/* posix_pty -->**PLANNED** — Linux Machine PTY;
  <!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer signals,files,ports,snapshot,suspend,checkpoint -->**PLANNED** —
  signals, files, ports, snapshot, suspend and checkpoint capabilities on every
  live pair.
- <!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer network_private,network_simulated_public,endpoint,split_dns,tls_ingress,nat_firewall,host_import,host_export,egress_policy,faults,peering,workspace_read_write,workspace_read_only,workspace_snapshot,secret_bindings -->**PLANNED**
  — private/public-like topology networking, per-Machine endpoints, host
  imports/exports, egress policy, faults, peering, workspace projections and
  secret bindings; <!-- capability-matrix: macos-arm64/linux/developer volumes -->**PLANNED** — declared volumes for
  Linux Developer Machines.
- <!-- capability-matrix: linux-*/linux/*,windows-*/linux/*,windows-*/windows/developer pair -->**PLANNED**
  — Linux hosts, then Linux Machines and Environment topologies on Windows, then
  native Windows Developer Machines. Additional placement, collaboration, policy,
  and hosted capabilities preserve the same explicit target and identity model.
- <!-- capability-matrix: host:macos-x86_64 -->**NA** — Intel Macs;
  <!-- capability-matrix: macos-arm64/macos/hardened,windows-*/windows/hardened pair -->**NA** —
  Hardened native macOS and native Windows Machines;
  <!-- capability-matrix: macos-arm64/windows/*,linux-*/macos/*,linux-*/windows/*,windows-*/macos/* pair -->**NA**
  — native targets on hosts that cannot provide them.

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
- Tag capabilities **ACTIVE**, **DEV**, **PLANNED**, or **NA** exactly as
  `config/host-target-capabilities-v0.4.json` does and require real
  host×Machine-target end-to-end evidence before declaring parity.
- Lead with reproducibility and parallel environments; describe lockdown as one
  selectable security posture, not the entire reason the product exists.
