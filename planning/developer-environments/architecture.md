# Architecture: project-scoped Developer Environments

## Object model

```text
Developer Environment (stable public identity)
├── immutable TargetSpec { os, arch, image/version, requirements }
├── host record, class, lifecycle, capabilities, and ownership
├── HostBackend selected from (host OS, target OS, architecture)
├── TargetAdapter for native boot/process/service/shutdown semantics
├── workspace and persistent state
├── environment-scoped network/share/credential/endpoint policy
└── target-qualified capabilities
    ├── Linux: implicit private Docker/containerd/BuildKit + youki
    ├── macOS: native launchd/process/APFS/VM capabilities
    └── Windows: native service/process/NTFS/isolation capabilities
```

`Sandbox` remains a low-level runtime-contract name for an isolation boundary during migration. It is not a peer user journey. A Linux workspace may be a resident container; macOS and Windows workspaces are target-native. `Docker container` specifically means a workload created inside a Docker-capable Linux target.

## Identity

An environment has one stable ID. It is explicitly named or bound to a canonical project/worktree identity. Display names are human-readable; storage, endpoint-routing, and context keys are bounded, collision-checked identifiers owned by `vz-runtimed`. Persisted identity is not the native path string, so moving a project or crossing path syntaxes does not silently create or alias an environment.

The target is persisted independently of detected host data and cannot change in place. The daemon selects a backend only for a supported `(host, target, arch)` tuple. `vz status --json` reports host, target, backend diagnostics, target image/build, architecture, and negotiated capabilities.

Users never construct transport paths. Capability endpoints are backend-owned. For Docker-capable Linux targets, the Engine Endpoint Adapter preserves Docker streams and connection hijacking while authorizing/translating native host bind paths into Linux paths.

## Lifecycle

Environment lifecycle distinguishes stopped from deleted:

```text
Creating -> Ready <-> Stopped -> Deleting -> Deleted
    |          |         |          |
    +----------+---------+--------> Failed
```

- `stop` shuts down live compute and endpoints while retaining identity and persistent state, including Docker state and context for Linux targets.
- `restart` preserves identity and persistent state.
- `delete` is the destructive operation and removes only resources proven to belong to that environment.
- Direct Docker access to a stopped, missing, ambiguous, or unauthorized environment fails closed and never falls back to Docker Desktop or another environment.

## Target capabilities

Docker is a default Linux-target capability, not a universal Environment invariant. Linux environment reconciliation installs/verifies pinned artifacts, prepares private storage/configuration, registers the private endpoint/context, and may socket-activate dockerd/containerd on first API request.

macOS targets use native macOS process, launchd, filesystem, network, update, and VM lifecycle semantics. They do not spawn Linux just to provide Docker. Windows targets later use Windows-native process, service, console, filesystem, network, and isolation semantics rather than OCI/youki.

## Runtime boundary

- Linux Developer targets select the Developer kernel/profile explicitly; it includes `USER_NS` and required Docker/cgroup/networking capabilities.
- The public name `Hardened` replaces `Container` over a compatibility window. Hardened remains restricted and does not acquire Developer/Docker capabilities.
- Linux-on-macOS may run dockerd rootful inside the Linux environment VM because that VM is the host isolation boundary.
- Linux parity uses the rootless-in-sandbox design so no host-root Docker daemon is introduced.
- macOS Developer targets select pinned macOS image/build/channel metadata and a native macOS TargetAdapter.
- Windows-on-Windows uses a separately selected native Windows backend; the public API does not assume OCI, POSIX, Unix sockets, or a hypervisor.
- youki remains the sole OCI runtime in Linux targets only.

## Host selection UX

Linux-target creation automatically creates or repairs a Docker context, but never mutates Docker's global default context. Supported selection paths are:

```bash
vz dev docker -- ps
docker --context "$(vz dev context)" ps
eval "$(vz dev env)"   # exports a session-scoped DOCKER_CONTEXT
```

The first form executes the host's installed Docker CLI with the resolved context; it is not an Engine API translation shim. The second proves verbatim client compatibility. The third is convenient for a project shell.

These commands are host-neutral. Docker contexts hide whether the backend endpoint is a Unix socket, named pipe, or another authenticated local transport.

Targets without the Docker capability return a structured unsupported-capability error. Target-native shell/exec/status/stop/restart/delete remain universal.

## Host backend boundaries

- `HostBackend`: isolation, disks, endpoint publication, shares, and host networking.
- `TargetAdapter`: native boot, process/console, service supervision, and shutdown.
- `CapabilitySet`: negotiated Docker/OCI, POSIX TTY/signals, Windows console, sharing, ports, suspend, snapshot, and checkpoint behavior.
- `StorageBackend`: persistent environment and Docker state.
- `ShareBackend`: authorize native host paths and translate them to Linux environment paths.
- `NetworkBackend`: egress, host aliases, and published TCP/UDP ports.
- `EngineEndpointBackend`: publish/remove the Docker endpoint, enforce permissions, and reconnect.
- `TargetSupervisor`: reconcile Linux services, macOS launchd services, or future Windows services and readiness.

Virtualization.framework, VirtioFS, vsock, Linux namespaces, WSL distribution IDs, Hyper-V sockets, Windows isolation APIs, gateway IPs, and physical endpoint names remain backend diagnostics. They do not appear in portable configuration or normal lifecycle APIs.

## Compatibility and migration

- Existing `vz init/run/stop/status/logs` project behavior remains as aliases while the `vz dev` namespace becomes canonical.
- Existing Sandbox APIs remain available to runtime consumers, with explicit environment class added rather than inferred from ID prefixes or labels.
- `vz docker` translation behavior is deprecated separately; a context-selecting passthrough may exist only if its behavior is clearly distinct and it invokes the host Docker CLI.
- Old global socket references are invalid. No mutable “current environment” symlink is introduced because it races under parallel agents.
- Public claims distinguish active, development, and planned capabilities until the release-built E2E gate passes.
