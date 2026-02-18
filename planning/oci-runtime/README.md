# OCI Runtime — Planning Overview

## Vision

Extend vz with an embedded OCI container runtime that runs Linux containers in hardware-isolated VMs on macOS. Each container gets its own lightweight Linux VM (<2s cold start), with the container's rootfs mounted via VirtioFS. Combined with vz's existing macOS sandbox support, this gives a unified Rust API for running both Linux containers and macOS sandboxes — all backed by Apple's Virtualization.framework.

The goal: an embeddable, daemonless alternative to Docker Desktop that provides VM-level isolation per container.

## Problem Statement

Docker Desktop on macOS:
- Runs a single hidden Linux VM, all containers share one kernel
- Is a heavy daemon (Docker Engine + containerd + VM)
- Is not embeddable — you talk to it over a socket, not a library call
- Has licensing restrictions for enterprise use
- Provides container-level isolation (namespaces/cgroups), not VM-level

What we want:
- **Embeddable Rust library** — `runtime.run("python:3.12", config)` in your code
- **VM-per-container** — Hardware isolation, not shared kernel
- **No daemon** — Library manages VMs directly via Virtualization.framework
- **OCI-compatible** — Pull from Docker Hub, GHCR, private registries
- **Dual-platform** — Same API for Linux containers and macOS sandboxes
- **Fast** — <2s cold start for Linux containers, <10s restore for macOS

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      vz-oci                             │
│  Runtime, Container, ImageStore                         │
│  OCI image pull/unpack, container lifecycle             │
├──────────────────────┬──────────────────────────────────┤
│    vz-linux          │         vz-sandbox               │
│  Linux kernel +      │    macOS VM pool + sessions      │
│  initramfs, rootfs   │    (existing, unchanged)         │
│  via VirtioFS        │                                  │
├──────────────────────┴──────────────────────────────────┤
│                        vz                               │
│  Safe Rust API — boot_linux() + boot_macos()            │
├─────────────────────────────────────────────────────────┤
│              Virtualization.framework                   │
└─────────────────────────────────────────────────────────┘
```

### Crate Responsibilities

| Crate | New? | Responsibility |
|-------|------|----------------|
| `vz` | No | Safe VM API. Already supports `boot_linux()`. No changes needed. |
| `vz-sandbox` | No | macOS sandbox pool/sessions. No changes needed. |
| `vz-linux` | **Yes** | Minimal Linux kernel + initramfs management, Linux VM bootstrap, rootfs-via-VirtioFS. |
| `vz-oci` | **Yes** | OCI image pulling, layer unpacking, container lifecycle, unified Runtime API. |
| `vz-cli` | Extend | Add `vz run <image>` for Linux containers, extend existing commands. |

### Dependency Graph

```
vz-oci
  ├── vz-linux    (Linux container backend)
  ├── vz-sandbox  (macOS sandbox backend)
  └── vz          (VM primitives + vz::protocol shared types)

vz-linux
  └── vz          (boot_linux, VirtioFS, vsock)

vz-cli
  └── vz-oci      (unified runtime)
```

**Shared types**: `ExecOutput`, `ExecStream`, `ExecEvent`, `Channel`, `ResourceStats`, and the `Request`/`Response` protocol types live in `vz::protocol` (a module within the `vz` crate). Both `vz-sandbox` and `vz-oci` import them from there. See `05-base-prerequisites.md` for details.

**New workspace crates**: `vz-linux` and `vz-oci` must be added to `crates/Cargo.toml` workspace members and dependencies.

## Key Design Decisions

### 1. One VM per container (default)

Each container runs in its own Linux VM with its own kernel. This provides hardware-level isolation — a compromised container cannot affect other containers or the host. The overhead per VM is ~64-128 MB RAM for the Linux kernel + guest agent.

This is the right default for agent sandboxing (HQ's use case). A future shared-VM mode could be added for high-density workloads where isolation matters less.

### 2. No disk images for Linux containers

The container's OCI rootfs is shared from the host via VirtioFS. Writes go to a tmpfs overlay inside the VM. This means:
- No disk image creation (instant)
- No disk space consumed per running container
- Multiple containers from the same image share read-only base layers
- Container teardown = stop VM (no cleanup)

### 3. Same guest agent protocol

The guest agent inside Linux VMs uses the exact same vsock protocol (handshake, exec, streaming, etc.) as macOS VMs. Same port (7424), same wire format, same Rust types. The host doesn't need to know which OS the guest is running.

### 4. OCI-compatible, not OCI-compliant

We pull standard OCI images from standard registries. But we don't implement the full OCI runtime spec (config.json lifecycle hooks, all namespace types, etc.) because we're running VMs, not containers. The VM provides the isolation — we don't need cgroups, seccomp, or Linux namespaces.

### 5. macOS uses golden images, not OCI

macOS "containers" use the existing golden image model (vz-sandbox). There is no OCI ecosystem for macOS images, and Apple's EULA restricts macOS redistribution. The Runtime API abstracts over both backends — the caller doesn't need to care which one is used.

## Implementation Phases

### Phase 1: vz-linux

Build the Linux VM bootstrap layer. This is the foundation.

- Compile and ship a minimal arm64 Linux kernel (~10 MB)
- Build a minimal initramfs with busybox + guest agent
- Boot sequence: kernel → init → VirtioFS mount → overlayfs → switch_root → guest agent
- Target: <1s from `Vm::start()` to guest-agent-reachable, <2s total cold start
- Guest agent compiled for `aarch64-unknown-linux-musl` (static)

### Phase 2: vz-oci

Build the OCI runtime layer on top of vz-linux.

- Image pulling from registries (Docker Hub, GHCR, private)
- Layer caching and unpacking to `~/.vz/oci/`
- Rootfs assembly from layers (overlayfs inside VM)
- Container lifecycle: create, start, exec, stop, remove
- Runtime and Container Rust API

### Phase 3: Unified CLI

Extend vz-cli to support Linux containers.

- `vz run ubuntu:24.04 -- cargo build` (Linux container)
- `vz run --macos -- swift build` (macOS sandbox)
- `vz images` (list cached OCI images)
- `vz images prune` (clean up unused layers)

### Phase 4: Docker Socket (Future)

Expose a Docker Engine API on a Unix socket so `docker` CLI and `docker-compose` work against vz. This is the viral open-source play but is a significant amount of work and should come after the core is proven.

## Planning Documents

| Document | Covers |
|----------|--------|
| `01-linux-vm.md` | vz-linux crate: minimal kernel, initramfs, boot sequence, guest agent for Linux, resource defaults, distribution |
| `02-oci-images.md` | OCI image pulling, authentication, layer unpacking, image store, rootfs assembly, caching |
| `03-container-lifecycle.md` | vz-oci crate: Runtime and Container API, container lifecycle, networking, port forwarding, environment, mounts |
| `04-unified-api.md` | Unified Runtime API abstracting Linux and macOS backends, image reference routing, CLI integration, HQ integration |
| `05-base-prerequisites.md` | Changes required in base vz crates before OCI runtime implementation (Cargo.toml features, protocol extensions, shared types) |

## Constraints

| Constraint | Detail |
|-----------|--------|
| **macOS host only** | Virtualization.framework is macOS-only. This runtime only works on macOS hosts. |
| **Apple Silicon only** | arm64 Linux kernel, arm64 container images. No x86_64 image support (Rosetta translation is possible but adds complexity). |
| **No GPU passthrough** | Linux VMs cannot access Metal/GPU. ML training workloads that need GPU won't work inside these VMs. |
| **No nested containers** | Cannot run Docker inside a vz Linux VM (no nested virtualization). |
| **arm64 images only** | Only `linux/arm64` OCI images are supported. `linux/amd64` images require Rosetta (future). |
