# vz — Master Architecture Overview

## Vision

vz is a Rust-native library for Apple's Virtualization.framework, purpose-built for sandboxing coding agents in macOS virtual machines. It provides a safe async Rust API over the framework's Objective-C classes, a high-level sandbox abstraction for agent runtimes, and a CLI for standalone use. The goal: any coding agent on macOS (Claude Code, Codex, OpenCode, Aider) gets full OS-level isolation in a native macOS VM with sub-10-second restore times and zero network configuration.

## Problem Statement

Every coding agent running on macOS needs sandboxing. Agents execute arbitrary code, install packages, modify filesystems, and make network calls. Without isolation, a single hallucinated `rm -rf /` or exfiltrated credential is catastrophic.

**Why Linux VMs don't solve this.** Tools like Vibe, VibeBox, and Claude Cowork run agents in Linux micro-VMs. But macOS development requires macOS: Rust binaries compiled for darwin, Xcode toolchains, SwiftUI previews, macOS-specific APIs, Homebrew packages. Cross-compiling from Linux adds friction and breaks half the toolchain.

**Why existing macOS tools don't solve this:**

- **Seatbelt (sandbox-exec)** — Deprecated by Apple. Process-level only, no filesystem isolation, no network domain filtering, known escape vectors. Not suitable for adversarial workloads.
- **Tart** — Excellent macOS VM manager for CI, but written in Swift with no Rust API. Designed for ephemeral clones, not long-lived sandboxes with fast session turnover. No vsock-based communication channel.
- **Docker** — Runs Linux containers on macOS via a hidden Linux VM. Same cross-compilation problem. Not native macOS isolation.
- **Kata Containers / Firecracker** — Linux-only hypervisors. No macOS guest support.

There is no Rust library for running macOS VMs as coding agent sandboxes. vz fills that gap.

## Architecture

### Stack Diagram

```
┌─────────────────────────────────────────────────────┐
│                     vz-cli                          │
│  `vz run --image base --mount project:./workspace`  │
├─────────────────────────────────────────────────────┤
│                   vz-sandbox                        │
│  Pool, Session, Channel, Guest Agent binary         │
├─────────────────────────────────────────────────────┤
│                      vz                             │
│  Safe Rust: Vm, Config, VirtioFs, Vsock, SaveState  │
├─────────────────────────────────────────────────────┤
│              objc2-virtualization v0.3.2             │
│  Auto-generated bindings to ALL Vz.framework classes│
├─────────────────────────────────────────────────────┤
│           Apple Virtualization.framework            │
│              (macOS 14+ / Apple Silicon)            │
└─────────────────────────────────────────────────────┘
```

### Host/Guest Communication

```
┌─────────────── HOST ───────────────┐     ┌─────────────── GUEST ──────────────┐
│                                    │     │                                     │
│  Agent Runtime (HQ, Claude Code)   │     │  Guest Agent (vz-guest-agent)       │
│       │                            │     │       │                             │
│       ▼                            │     │       ▼                             │
│  vz-sandbox::Channel               │     │  vsock listener (port 7424)         │
│       │                            │     │       │                             │
│       ▼                            │     │       ▼                             │
│  vsock (port 7424)  ◄──────────────┼─────┼──  vsock device                     │
│                                    │     │                                     │
│  VirtioFS share ────────────────── │ ──► │  /mnt/workspace (auto-mounted)      │
│  (host: ./my-project)              │     │  (read-write project files)         │
│                                    │     │                                     │
└────────────────────────────────────┘     └─────────────────────────────────────┘

Communication:  vsock — length-prefixed JSON frames (no SSH, no network config)
File sharing:   VirtioFS — near-native performance, configured at VM creation
```

### Crate Responsibilities

- **vz** wraps `objc2-virtualization` in safe async Rust. It owns VM lifecycle (create, start, pause, stop, save, restore), configuration building, VirtioFS mounts, and vsock streams. All ObjC completion handlers are bridged to tokio futures. All VZVirtualMachine operations are dispatched through a serial dispatch queue (mandatory Apple requirement).

- **vz-sandbox** provides the high-level abstraction: a pool of pre-warmed VMs, session lifecycle (acquire a VM, mount a project, execute commands, release), and a typed Channel protocol over vsock. It also contains the guest agent binary that runs inside the VM.

- **vz-cli** exposes vz-sandbox functionality as CLI commands for standalone use without writing Rust.

## Crate Overview

| Crate | Responsibility | Key Types |
|-------|---------------|-----------|
| `vz` | Safe async Rust API over objc2-virtualization | `Vm`, `VmConfig`, `VmConfigBuilder`, `VirtioFsMount`, `VsockStream`, `VsockListener`, `VmState`, `SaveState` |
| `vz-sandbox` | High-level sandbox pool, sessions, guest agent | `SandboxPool`, `SandboxSession`, `Channel<Req, Resp>`, `GuestAgent`, `SandboxConfig` |
| `vz-cli` | CLI for standalone use | Clap commands: `init`, `run`, `exec`, `save`, `restore`, `list`, `stop` |

## Key Design Decisions

### 1. objc2-virtualization as foundation — no vz-sys crate

The `objc2-virtualization` crate (v0.3.2) provides auto-generated bindings to ALL Virtualization.framework classes. These bindings are mechanically generated from Apple's headers by the objc2 project, covering every class from `VZVirtualMachine` to `VZVirtioSocketConnection`. This eliminates the need for a separate `vz-sys` crate with hand-written FFI bindings. The `vz` crate wraps `objc2-virtualization` directly with a safe, ergonomic API. The project has 3 crates, not 4.

### 2. Async-first with tokio

All Virtualization.framework operations use Objective-C completion handlers (blocks). We bridge these to Rust futures using `block2` to create the ObjC block and `tokio::sync::oneshot` to deliver the result. Every public method on `Vm` returns a `Future`. The runtime is tokio.

### 3. Serial dispatch queue per VM — MANDATORY

Apple requires that ALL `VZVirtualMachine` operations (start, stop, pause, resume, save, restore, delegate callbacks) execute on the same serial dispatch queue. This is not optional — violating it causes undefined behavior or crashes. We use the `dispatch2` crate to create a serial queue per VM and ensure every ObjC call is dispatched through it. This is the single most important correctness requirement in the crate.

### 4. macOS 14 (Sonoma) minimum

Save/restore (`saveMachineStateTo:completionHandler:` / `restoreMachineStateFrom:completionHandler:`) was introduced in macOS 14. Without it, every sandbox session requires a full 30-60s macOS boot. With it, restore takes 5-10s. This is essential for the sandbox use case, so we set macOS 14 as the floor.

### 5. Long-lived VM model

Rather than ephemeral clone-and-boot (Tart's CI model), vz uses a single long-lived VM. The VM boots once (or restores from saved state), then serves sessions sequentially. Project directories are swapped via VirtioFS mounts. This avoids the 2-VM kernel limit being a bottleneck, eliminates boot penalties, and sidesteps APFS clone management.

### 6. vsock for host-guest communication — not SSH

vsock (`AF_VSOCK`) provides a socket interface between host and guest without any network configuration. No IP addresses, no SSH keys, no port forwarding. The host connects to the guest (or vice versa) using a simple port number. This maps perfectly to tool-forwarding architectures where the host holds secrets and the guest holds only stubs.

### 7. Push-based streaming

stdout/stderr from guest command execution is streamed to the host as vsock frames in real time. The guest agent pushes output as it arrives rather than the host polling for it. Frame format: 4-byte little-endian length prefix followed by a JSON payload containing the stream type (stdout/stderr/exit) and data.

### 8. Measured claims only

Performance and reliability claims must be backed by reproducible benchmark evidence.
Any headline claim (startup time, restore time, pull speed, convergence latency, or
density) must include the exact host/software baseline, benchmark method, and raw
artifacts needed for independent verification.

## Implementation Phases

### Phase 1: vz crate — Safe API over objc2-virtualization

Build the safe Rust wrapper. This is the foundation everything else depends on.

- Dispatch queue management (serial queue per VM, `dispatch2` crate)
- Async bridging pattern (ObjC completion handler to tokio future via `block2` + `oneshot`)
- `VmConfigBuilder`: CPU count, memory, boot loader, disk images, VirtioFS mounts, vsock, network
- `Vm` lifecycle: create, start, pause, resume, stop
- VirtioFS: shared directory configuration, read-only vs read-write
- Vsock: `VsockStream` (AsyncRead + AsyncWrite), `VsockListener`
- Save/restore VM state (macOS 14+)
- macOS installer: download IPSW, install to disk image
- Delegate implementation for VM state changes

### Phase 2: vz-sandbox + Guest Agent

Build the high-level abstraction and the binary that runs inside the VM.

- Guest agent binary (`vz-guest-agent`): listens on vsock, executes commands, streams output
- Guest agent bootstrap: launchd plist, auto-start on boot, VirtioFS auto-mount
- `SandboxPool`: pre-warm VMs (up to 2), manage lifecycle
- `SandboxSession`: acquire VM, mount project, execute commands, release
- `Channel<Req, Resp>`: typed request/response protocol over vsock with length-prefixed JSON frames
- Session cleanup: process termination, temp file removal between sessions

### Phase 3: vz-cli

CLI binary for standalone use.

- `vz init` — Download IPSW, create golden disk image, install macOS, install dev tools
- `vz run` — Start VM with mounts, optional headless mode
- `vz exec` — Run a command inside a running VM via guest agent
- `vz save` / `vz restore` — Snapshot and restore VM state
- `vz list` — Show running VMs and their state
- `vz stop` — Graceful shutdown

### Phase 4: Ecosystem

- Pre-built golden images with common dev tools (Xcode CLI, Homebrew, Rust, Node, Python)
- CI integration guides (GitHub Actions on Apple Silicon runners)
- Integration examples: HQ kernel worker backend, Claude Code sandbox, generic agent harness
- OCI registry for distributing golden images

## Phase Dependency Graph

```
Phase 1: vz crate
    │
    ├──────────────────────┐
    ▼                      ▼
Phase 2: vz-sandbox    Phase 3: vz-cli
    │                      │
    └──────┬───────────────┘
           ▼
    Phase 4: Ecosystem
```

Phase 1 must complete before Phase 2 or Phase 3 can begin. Phase 2 and Phase 3 can proceed in parallel (vz-cli can use the vz crate directly for basic VM operations before vz-sandbox exists). Phase 4 depends on both Phase 2 and Phase 3.

## Constraints & Limitations

| Constraint | Detail |
|-----------|--------|
| **Apple Silicon only** | macOS guest VMs require Apple Silicon (arm64). Intel Macs cannot run macOS guests. |
| **2 concurrent macOS VMs** | Kernel-enforced limit. The long-lived VM model makes this irrelevant for most use cases. |
| **macOS host only** | Virtualization.framework is a macOS-only framework. No Linux or Windows host support. |
| **No nested virtualization** | Cannot run VMs inside the sandbox VM. Agents cannot use Docker inside the guest. |
| **No Metal passthrough** | No GPU acceleration in guests. GPU-dependent workloads (ML training, Metal shaders) will not work. |
| **VirtioFS mounts are static** | Shared directories must be configured at VM creation time. Cannot add or remove mounts while the VM is running. To change mounts, the VM must be stopped and recreated. |
| **Hardware-encrypted save files** | VM state save files are encrypted by the Secure Enclave. They are tied to the specific Mac hardware and user account. Not portable between machines. |
| **macOS 14 minimum** | Save/restore requires Sonoma or later. Older macOS versions can run VMs but cannot snapshot/restore state. |

## Planning Documents Index

| Document | Covers |
|----------|--------|
| `00-ffi-layer.md` | objc2-virtualization usage patterns, serial dispatch queue requirement, async bridging with block2 + oneshot, delegate implementation strategy |
| `01-safe-api.md` | vz crate design: Vm lifecycle, VmConfigBuilder, error types, state machine, public API surface |
| `02-virtio-fs.md` | VirtioFS mount strategy, workspace root pattern, read-only vs read-write, mount tag naming, guest-side auto-mount |
| `03-vsock-protocol.md` | vsock communication model, wire format (length-prefixed JSON), port assignments, streaming protocol, backpressure, **connection handshake/version negotiation, base64 binary encoding, ResourceStats** |
| `04-guest-agent.md` | Guest agent binary architecture, bootstrap via launchd, command execution, environment setup, **non-root execution via user field, vsock security model, connection draining** |
| `05-sandbox.md` | vz-sandbox crate: SandboxPool lifecycle, SandboxSession acquire/release, Channel typed protocol, **session isolation (RestoreOnAcquire vs Reuse), exec timeouts, network isolation policy** |
| `06-cli.md` | vz-cli commands, UX design, golden image creation workflow, interactive vs headless modes, **orphaned VM detection, distribution channels** |
| `07-golden-image.md` | IPSW to bootable macOS VM pipeline, dev tool provisioning, image versioning, **automated first-boot provisioning (skip Setup Assistant, pre-create user), fully unattended vz init flow** |
| `08-testing.md` | Testing strategy for a virtualization library: unit tests (mock ObjC), integration tests (real VMs), CI on Apple Silicon, and measured-claims benchmark requirements |
| `09-signing.md` | **Entitlements (com.apple.security.virtualization), code signing with Developer ID, notarization, CI signing workflow, distribution (Homebrew, GitHub Releases, cargo install, install script)** |
| `../docs/runtime-api-review.md` | Runtime/product boundary enforcement checklist for contract/API changes, extension guardrails, and redesign triggers |
| `runtime-v2-rollout.md` | Runtime V2 phased rollout tracker (Phase 0-6) with entry/exit gates, ownership, evidence requirements, and Beads linkage |
| `runtime-v2-risks.md` | Runtime V2 rollout risk register with mitigation execution tracking, acceptance criteria, and review cadence |
| `on-demand-elevation/README.md` | One-command UX with stage-gated elevation: request admin privileges only when offline provisioning requires it; includes security, UX, and rollout plan |
| `pinned-ipsw-patches/README.md` | Pinned base matrix + signed file-level patch bundle strategy to keep system reliability while enabling no-local-sudo artifact workflows |
| `agent-loader-bootstrap/README.md` | Stage-0 loader + signed swappable guest-agent artifact plan to avoid frequent image deltas for agent updates |
| `sandbox/README.md` | Agent sandbox platform track: first-class primitives (`Sandbox`, `Session`, `Run`, `Policy`), Rust library surface, runtime integration plan, and OpenAPI contract |
| `docker-in-sandbox/README.md` | Full Docker-inside-sandbox platform track: core primitives (`EngineInstance`, `EndpointLease`, filesystem and policy model) and reusable infrastructure surface across SDK/CLI/OpenAPI |
| `oci-runtime/README.md` | Linux OCI runtime track (current): Linux VM bootstrap, image pulling, container lifecycle, unified API prerequisites |
| `oci-youki-compose/README.md` | OCI runtime-spec compliance via `youki` + Compose-class stack runtime/reconciler track |

## Prior Art

| Project | Language | Relevance |
|---------|----------|-----------|
| [Tart](https://github.com/cirruslabs/tart) | Swift | Most mature macOS VM manager. CI-focused, ephemeral clone model. No Rust API. Reference for IPSW install flow and VM configuration. |
| [vfkit](https://github.com/crc-org/vfkit) | Go | Minimal Virtualization.framework wrapper from Red Hat. Linux guests only. Clean API design reference. |
| [Vibe](https://github.com/lynaghk/vibe) | Rust | Linux VM sandbox for coding agents. Proves the agent-in-VM model works. Linux-only. |
| [VibeBox](https://github.com/robcholz/vibebox) | Rust | Per-project Linux micro-VMs. Similar sandbox concept, different OS target. |
| [Code-Hex/vz](https://github.com/Code-Hex/vz) | Go | Most complete Virtualization.framework binding outside Swift. Mature, well-tested. Primary API design reference. |
| [objc2](https://github.com/madsmtm/objc2) | Rust | Safe Rust-to-ObjC interop. Foundation of our binding strategy. Includes `objc2-virtualization` with auto-generated bindings to all Vz.framework classes. |
| [objc2-virtualization](https://docs.rs/objc2-virtualization) | Rust | Auto-generated bindings to ALL Virtualization.framework classes (v0.3.2). Eliminates the need for hand-written FFI. Our direct dependency. |
| [Claude Cowork](https://docs.anthropic.com) | — | Anthropic's agent sandbox. Uses Linux VMs. Demonstrates the vsock + VirtioFS communication pattern we adopt for macOS. |
| [Kata Containers](https://katacontainers.io) | Go | VM-based container isolation. Architectural reference for the pool/session model, though Linux-only. |
| [Firecracker](https://github.com/firecracker-microvm/firecracker) | Rust | AWS micro-VM hypervisor. Reference for fast VM startup and minimal attack surface design. Linux KVM only. |
