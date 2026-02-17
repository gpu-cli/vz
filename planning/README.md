# vz — macOS Virtualization.framework for Rust

## Vision

A Rust-native interface to Apple's Virtualization.framework, purpose-built for sandboxing coding agents in macOS VMs. Three layers:

1. **vz-sys** — Raw Objective-C FFI bindings
2. **vz** — Safe, ergonomic Rust API
3. **vz-sandbox** — High-level "mount a folder, run an agent, tear down" abstraction

Plus a CLI (`vz-cli`) for standalone use without writing Rust.

## Problem Statement

Every coding agent on macOS (Claude Code, Codex, OpenCode, Aider) needs sandboxing. Today's options:

- **Seatbelt (sandbox-exec)** — deprecated, process-level only, no network domain filtering, known escape vectors
- **Linux VMs** (Vibe, VibeBox, Claude Cowork) — can't run macOS binaries, can't test macOS APIs, forces cross-compilation
- **Tart** — excellent for CI but written in Swift, no Rust API, designed for ephemeral clones not long-lived sandboxes
- **Docker** — Linux containers on macOS, same cross-compilation problem

There is no Rust library for running macOS VMs as coding agent sandboxes. This project fills that gap.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     vz-cli                          │
│  `vz run --image base --mount project:./workspace`  │
├─────────────────────────────────────────────────────┤
│                   vz-sandbox                        │
│  Pool, Session, Channel — high-level sandbox API    │
├─────────────────────────────────────────────────────┤
│                      vz                             │
│  Safe Rust: Vm, Config, VirtioFs, Vsock, SaveState  │
├─────────────────────────────────────────────────────┤
│                    vz-sys                           │
│  Raw FFI: objc2 bindings to Virtualization.framework│
├─────────────────────────────────────────────────────┤
│           Apple Virtualization.framework            │
│              (macOS 12+ / Apple Silicon)            │
└─────────────────────────────────────────────────────┘
```

## Crate Design

### vz-sys — Raw FFI Bindings

Thin, unsafe bindings to Virtualization.framework using `objc2` + `block2`.

**Bound classes (v0.1 scope):**

| ObjC Class | Purpose |
|-----------|---------|
| `VZVirtualMachine` | VM lifecycle (start, pause, stop, save, restore) |
| `VZVirtualMachineConfiguration` | CPU, memory, devices |
| `VZMacOSBootLoader` | macOS guest boot |
| `VZMacPlatformConfiguration` | Machine identity, hardware model, aux storage |
| `VZMacOSRestoreImage` | IPSW download + install |
| `VZMacOSInstaller` | Install macOS into a VM |
| `VZVirtioFileSystemDeviceConfiguration` | VirtioFS shared directories |
| `VZSharedDirectory` | Single directory share |
| `VZMultipleDirectoryShare` | Multiple directory shares |
| `VZVirtioSocketDeviceConfiguration` | vsock setup |
| `VZVirtioSocketDevice` | vsock connections |
| `VZVirtioSocketConnection` | Individual vsock connection |
| `VZDiskImageStorageDeviceAttachment` | Disk images |
| `VZVirtioBlockDeviceConfiguration` | Block storage |
| `VZNATNetworkDeviceAttachment` | NAT networking |
| `VZVirtioNetworkDeviceConfiguration` | Network adapter |
| `VZMacGraphicsDeviceConfiguration` | Display (optional, for debug) |

**Build requirements:**
- `build.rs` links `Virtualization.framework`
- `#[cfg(target_os = "macos")]` gate on everything
- Minimum deployment target: macOS 14 (Sonoma) for save/restore

### vz — Safe Rust API

Ergonomic wrapper. Key types:

```rust
/// VM configuration builder
pub struct VmConfigBuilder {
    cpus: u32,
    memory_bytes: u64,
    boot_loader: BootLoader,
    disks: Vec<DiskConfig>,
    shared_dirs: Vec<SharedDirConfig>,
    network: Option<NetworkConfig>,
    vsock: bool,
}

/// A running or stopped virtual machine
pub struct Vm { /* opaque, wraps VZVirtualMachine */ }

impl Vm {
    pub async fn start(&self) -> Result<()>;
    pub async fn pause(&self) -> Result<()>;
    pub async fn resume(&self) -> Result<()>;
    pub async fn stop(&self) -> Result<()>;

    /// Save full VM state to disk (macOS 14+). VM must be paused.
    pub async fn save_state(&self, path: &Path) -> Result<()>;

    /// Restore VM from saved state. Must use same configuration.
    pub async fn restore_state(&self, path: &Path) -> Result<()>;

    /// Get a vsock connection to the guest on the given port
    pub async fn vsock_connect(&self, port: u32) -> Result<VsockStream>;

    /// Listen for vsock connections from the guest
    pub async fn vsock_listen(&self, port: u32) -> Result<VsockListener>;

    /// Current VM state
    pub fn state(&self) -> VmState;
}

pub enum VmState {
    Stopped,
    Running,
    Paused,
    Starting,
    Stopping,
    Saving,
    Restoring,
    Error,
}

pub enum BootLoader {
    MacOS,
    Linux { kernel: PathBuf, initrd: Option<PathBuf>, cmdline: String },
}

/// Install macOS from IPSW into a disk image
pub async fn install_macos(ipsw: IpswSource, disk: &Path, config: &VmConfigBuilder) -> Result<()>;

pub enum IpswSource {
    Latest,            // Download latest from Apple
    Path(PathBuf),     // Local IPSW file
    Url(String),       // Remote URL
}
```

**VirtioFS API:**

```rust
pub struct SharedDirConfig {
    pub tag: String,                // Guest mount tag
    pub source: PathBuf,            // Host directory
    pub read_only: bool,
}

// Usage in config builder:
let config = VmConfigBuilder::new()
    .cpus(4)
    .memory_gb(8)
    .boot_macos()
    .disk("/path/to/disk.img", 64 * GB)
    .shared_dir("project", "./my-project", ReadWrite)
    .shared_dir("tools", "/usr/local/bin", ReadOnly)
    .enable_vsock()
    .build()?;
```

**Vsock API:**

```rust
/// Bidirectional byte stream over vsock
pub struct VsockStream { /* wraps VZVirtioSocketConnection */ }

impl tokio::io::AsyncRead for VsockStream { ... }
impl tokio::io::AsyncWrite for VsockStream { ... }

/// Accept incoming vsock connections from guest
pub struct VsockListener { /* wraps listener on VZVirtioSocketDevice */ }

impl VsockListener {
    pub async fn accept(&self) -> Result<VsockStream>;
}
```

### vz-sandbox — High-Level Sandbox

The "just give me a sandbox" layer.

```rust
/// A pool of pre-warmed macOS VMs ready for use
pub struct SandboxPool {
    config: SandboxConfig,
    available: Vec<Sandbox>,
}

pub struct SandboxConfig {
    pub image_path: PathBuf,        // Path to golden disk image
    pub cpus: u32,
    pub memory_gb: u32,
    pub state_path: Option<PathBuf>, // Saved state for fast restore
}

impl SandboxPool {
    /// Create pool and pre-warm VMs (up to 2 for macOS guests)
    pub async fn new(config: SandboxConfig, pool_size: u8) -> Result<Self>;

    /// Get a sandbox from the pool, mount a project directory
    pub async fn acquire(&self, project_dir: &Path) -> Result<SandboxSession>;

    /// Return sandbox to the pool after cleanup
    pub async fn release(&self, session: SandboxSession) -> Result<()>;
}

/// An active sandbox session with a mounted project
pub struct SandboxSession {
    vm: Vm,
    vsock: VsockStream,
    project_mount: String,  // tag for the VirtioFS mount
}

impl SandboxSession {
    /// Execute a command inside the sandbox via SSH or guest agent
    pub async fn exec(&self, cmd: &str) -> Result<ExecOutput>;

    /// Get the vsock channel for custom protocols (e.g., tool forwarding)
    pub fn channel(&self) -> &VsockStream;

    /// Path where the project is mounted inside the VM
    pub fn project_path(&self) -> &str; // e.g., "/mnt/project"
}

/// Typed message protocol over vsock for tool forwarding
pub struct Channel<Req, Resp> {
    stream: VsockStream,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Serialize, Resp: DeserializeOwned> Channel<Req, Resp> {
    pub async fn send(&self, req: Req) -> Result<()>;
    pub async fn recv(&self) -> Result<Resp>;
    pub async fn request(&self, req: Req) -> Result<Resp>;
}
```

### vz-cli — Command Line Interface

```
vz init                              # Download IPSW, create golden image
vz run --image base --mount project:./workspace
vz run --image base --mount project:./workspace --headless
vz save --image base --state base.state
vz restore --state base.state --mount project:./workspace
vz exec <vm-name> -- cargo build
vz list                              # Show running VMs
vz stop <vm-name>
```

## Key Design Decisions

### 1. objc2 for FFI (not manual objc_msgSend)

The `objc2` ecosystem provides:
- Compile-time class/method verification
- Automatic retain/release memory management
- Safe wrappers for Objective-C blocks (`block2` crate)
- Foundation type conversions (NSString, NSURL, NSError)

This is more work upfront than calling Tart's CLI, but produces a proper library that others can build on.

### 2. Async-first with tokio

All VM operations are async (start, stop, save, restore are completion-handler based in ObjC). We bridge to tokio futures using `block2` + `tokio::sync::oneshot`.

### 3. macOS 14 (Sonoma) minimum

We require macOS 14+ because save/restore (`saveMachineStateTo`/`restoreMachineStateFrom`) is essential for fast sandbox startup. Without it, every session requires a full 30-60s macOS boot.

### 4. Long-lived VM model (not ephemeral clones)

The primary use case is a single macOS VM that stays running. Project directories are swapped via VirtioFS mounts. This avoids:
- The 2-VM limit being a bottleneck
- 30-60s boot penalty per session
- APFS clone management complexity

The VM boots once (or restores from saved state in ~5-10s), then serves sessions sequentially.

### 5. vsock as primary communication channel

vsock provides host↔guest communication without network configuration. This maps perfectly to tool-forwarding architectures where the host holds secrets and the guest holds only stubs.

## Implementation Plan

### Phase 1: Foundation (vz-sys + vz)
1. Set up objc2 bindings for core Virtualization.framework classes
2. Implement VmConfigBuilder + Vm lifecycle (start/stop)
3. VirtioFS shared directory support
4. vsock host↔guest channel
5. macOS IPSW download + install
6. Save/restore VM state

### Phase 2: Sandbox Layer (vz-sandbox)
1. SandboxPool with pre-warming
2. SandboxSession with project mounting
3. Guest agent or SSH-based command execution
4. Typed Channel protocol over vsock

### Phase 3: CLI (vz-cli)
1. `vz init` — golden image creation
2. `vz run` — start VM with mounts
3. `vz exec` — run commands in VM
4. `vz save` / `vz restore` — state management

### Phase 4: Ecosystem
1. Guest agent binary (runs inside VM, listens on vsock)
2. Pre-built golden images (published to OCI registry)
3. Integration examples (HQ, Claude Code, generic agent)

## Constraints & Limitations

- **Apple Silicon only** — macOS guest VMs require Apple Silicon
- **2 concurrent macOS VMs** — kernel-enforced limit; the long-lived model makes this irrelevant
- **macOS host only** — Virtualization.framework is macOS-only
- **No nested virtualization** — can't run VMs inside the sandbox VM
- **No Metal passthrough** — no GPU acceleration in guests
- **VirtioFS mounts are static** — configured at VM creation, can't add/remove at runtime
- **Hardware-encrypted save files** — state files tied to specific Mac + user account, not portable

## Prior Art & References

- [Apple Virtualization.framework docs](https://developer.apple.com/documentation/virtualization)
- [WWDC22: Create macOS or Linux virtual machines](https://developer.apple.com/videos/play/wwdc2022/10002/)
- [WWDC23: Create seamless experiences with Virtualization](https://developer.apple.com/videos/play/wwdc2023/10007/)
- [Tart (Cirrus Labs)](https://github.com/cirruslabs/tart) — Swift, macOS+Linux VMs for CI
- [vfkit (Red Hat)](https://github.com/crc-org/vfkit) — Go, minimal Linux VM hypervisor
- [Vibe (lynaghk)](https://github.com/lynaghk/vibe) — Rust, Linux VM for agent sandbox
- [VibeBox](https://github.com/robcholz/vibebox) — Rust, per-project Linux micro-VMs
- [Code-Hex/vz](https://github.com/Code-Hex/vz) — Go bindings (mature, reference implementation)
- [virtualization-rs](https://github.com/suzusuzu/virtualization-rs) — Rust bindings (alpha)
- [objc2](https://github.com/madsmtm/objc2) — Safe Rust bindings for Objective-C
