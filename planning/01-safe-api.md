# 01 — `vz` Crate: Safe Rust API for Virtualization.framework

## Purpose

The `vz` crate wraps `objc2-virtualization` (auto-generated Objective-C bindings) with a safe, ergonomic Rust API. It is the core of the project.

Design goals:

- **Safety boundary** — All `unsafe` code is contained within this crate's internals. Consumers of `vz` never write `unsafe`. The public API is 100% safe Rust.
- **Ergonomic** — Fluent builders, standard Rust error handling via `Result`, idiomatic enums for state.
- **Async-first** — Built on tokio. All blocking/callback-based Virtualization.framework operations are wrapped as `async fn` returning futures. ObjC dispatch queues are hidden behind the async interface.
- **Typed** — No stringly-typed configuration. Enums, newtypes, and builder validation catch misconfiguration at compile time or at `.build()` time, not at VM boot.

### Relationship to `objc2-virtualization`

`objc2-virtualization` provides raw, auto-generated bindings to every class and method in Virtualization.framework. These bindings are `unsafe`, require manual memory management via `Retained<T>`, and demand correct dispatch queue usage. The `vz` crate exists so that no other crate in the workspace needs to touch any of that.

```
┌─────────────────────────────────┐
│  CLI / daemon / higher layers   │  ← safe Rust only
├─────────────────────────────────┤
│          vz (this crate)        │  ← safe public API, unsafe internals
├─────────────────────────────────┤
│     objc2-virtualization        │  ← raw ObjC bindings (unsafe)
├─────────────────────────────────┤
│   Virtualization.framework      │  ← Apple system framework
└─────────────────────────────────┘
```

---

## Module Structure

```
vz/src/
├── lib.rs          # Public API exports
├── config.rs       # VmConfigBuilder, BootLoader, DiskConfig, SharedDirConfig
├── vm.rs           # Vm struct, lifecycle methods
├── state.rs        # VmState enum, state change notifications
├── vsock.rs        # VsockStream, VsockListener (AsyncRead/AsyncWrite)
├── virtio_fs.rs    # VirtioFS mount configuration
├── install.rs      # macOS IPSW download and installation
├── error.rs        # VzError enum (thiserror)
└── bridge.rs       # Internal: dispatch queue, async bridging, delegate
```

### Module Responsibilities

| Module | Public | Description |
|--------|--------|-------------|
| `lib.rs` | yes | Re-exports the public API surface. Nothing else. |
| `config.rs` | yes | `VmConfigBuilder` (fluent builder), `VmConfig` (validated output), `BootLoader`, `DiskConfig`, `SharedDirConfig`. |
| `vm.rs` | yes | `Vm` struct with all lifecycle methods (`create`, `start`, `pause`, `resume`, `stop`, etc.). |
| `state.rs` | yes | `VmState` enum and its mapping from `VZVirtualMachineState`. |
| `vsock.rs` | yes | `VsockStream` (implements `AsyncRead` + `AsyncWrite`), `VsockListener`. |
| `virtio_fs.rs` | yes | `VirtioFsMount` configuration type and internal wiring to `VZVirtioFileSystemDeviceConfiguration`. |
| `install.rs` | yes | `IpswSource` enum, `install_macos` async function. |
| `error.rs` | yes | `VzError` enum with `thiserror` derives. |
| `bridge.rs` | **no** | Internal module. `DispatchQueue` wrapper, `define_class!` delegate, ObjC-to-tokio async bridging. Not re-exported. |

---

## VmConfigBuilder

Fluent builder pattern for constructing a validated `VmConfig`. All validation happens at `.build()` time — the builder itself stores raw values without checking them, so methods can be called in any order.

### Usage

```rust
let config = VmConfigBuilder::new()
    .cpus(4)
    .memory_gb(8)
    .boot_macos()
    .disk("/path/to/disk.img")
    .shared_dir(SharedDirConfig {
        tag: "workspace".into(),
        source: PathBuf::from("./workspace"),
        read_only: false,
    })
    .enable_vsock()
    .build()?;
```

### Builder Methods

| Method | Type | Default | Description |
|--------|------|---------|-------------|
| `.cpus(n)` | `u8` | `2` | Virtual CPU count. Validated: min 1, max `ProcessInfo.processorCount`. |
| `.memory_gb(n)` | `u64` | `4` | RAM in gigabytes. Converted to bytes internally. Validated: min 2 GB (framework minimum), max `ProcessInfo.physicalMemory`. |
| `.memory_bytes(n)` | `u64` | — | RAM in bytes. Alternative to `.memory_gb()`. Same validation. Last call wins. |
| `.boot_macos()` | — | — | Use `VZMacOSBootLoader`. Sets platform to `VZMacPlatformConfiguration` with hardware model, machine identifier, and auxiliary storage. Mutually exclusive with `.boot_linux()`. |
| `.boot_linux(kernel, initrd, cmdline)` | `PathBuf, Option<PathBuf>, String` | — | Use `VZLinuxBootLoader`. Sets platform to `VZGenericPlatformConfiguration`. Mutually exclusive with `.boot_macos()`. |
| `.disk(path)` | `impl Into<PathBuf>` | `[]` | Append a read-write block storage device backed by the disk image at `path`. Can be called multiple times for multiple disks. |
| `.disk_config(cfg)` | `DiskConfig` | `[]` | Append a disk with full configuration (read-only flag, cache mode, sync mode). |
| `.shared_dir(cfg)` | `SharedDirConfig` | `[]` | Add a VirtioFS shared directory. Can be called multiple times. Tags must be unique. |
| `.enable_vsock()` | — | disabled | Attach a `VZVirtioSocketDevice` to the VM. Required for `vsock_connect` / `vsock_listen`. |
| `.display(width, height, ppi)` | `u32, u32, u32` | none | Attach a virtual display (Mac graphics device). Optional. |
| `.network_nat()` | — | NAT | Use NAT networking (default). |
| `.network_bridged(iface)` | `&str` | — | Use bridged networking on the named host interface. |
| `.rosetta()` | — | disabled | Enable Rosetta translation for Linux guests on Apple Silicon. |
| `.mac_address(addr)` | `MacAddress` | random | Set a specific MAC address. If not called, the framework generates a random one. |

### Validation Rules (enforced at `.build()`)

- CPU count: `1 <= cpus <= host_physical_cores`. Returns `VzError::InvalidConfig` if violated.
- Memory: `2 GB <= memory <= host_physical_memory`. Returns `VzError::InvalidConfig` if violated.
- Boot loader: exactly one of `boot_macos()` or `boot_linux()` must be called. Missing boot loader is `InvalidConfig`.
- Shared directory tags: must be unique across all `shared_dir` calls. Duplicate tags are `InvalidConfig`.
- Disk paths: each path must exist and be a file (not a directory). Missing disk is `InvalidConfig`.
- VirtioFS source paths: each source must be an existing directory. Missing directory is `InvalidConfig`.

### Output: VmConfig

```rust
pub struct VmConfig {
    pub cpus: u8,
    pub memory_bytes: u64,
    pub boot: BootLoader,
    pub disks: Vec<DiskConfig>,
    pub shared_dirs: Vec<SharedDirConfig>,
    pub vsock_enabled: bool,
    pub display: Option<DisplayConfig>,
    pub network: NetworkConfig,
    pub rosetta: bool,
    pub mac_address: Option<MacAddress>,
}
```

`VmConfig` is a plain data struct. It is `Clone`, `Debug`, and `Serialize`/`Deserialize` (for persistence). It contains no ObjC references — those are created later in `Vm::create()`.

### Supporting Types

```rust
pub enum BootLoader {
    MacOs,
    Linux {
        kernel: PathBuf,
        initrd: Option<PathBuf>,
        cmdline: String,
    },
}

pub struct DiskConfig {
    pub path: PathBuf,
    pub read_only: bool,
    pub sync_mode: DiskSyncMode,
}

pub enum DiskSyncMode {
    Full,   // fsync after every write (safe, slower)
    None,   // no fsync (fast, risk of data loss on host crash)
}

pub struct SharedDirConfig {
    pub tag: String,           // mount tag visible inside the guest
    pub source: PathBuf,       // host directory to share
    pub read_only: bool,
}

pub struct DisplayConfig {
    pub width: u32,
    pub height: u32,
    pub ppi: u32,
}

pub enum NetworkConfig {
    Nat,
    Bridged { interface: String },
}
```

---

## Vm Struct

The core type. Wraps a `VZVirtualMachine` instance together with its required dispatch queue and delegate. This is the primary interface consumers interact with.

### Internal Structure

```rust
pub struct Vm {
    inner: Retained<VZVirtualMachine>,
    queue: DispatchQueue,
    delegate: Retained<VMDelegate>,
    state_rx: watch::Receiver<VmState>,
    config: VmConfig,
}
```

- `inner` — The ObjC virtual machine object. All method calls on it must happen on `queue`.
- `queue` — A serial `dispatch_queue_t` wrapped in a safe Rust type. Every ObjC call is dispatched here internally. The caller never interacts with it.
- `delegate` — An ObjC object defined via `define_class!` that implements `VZVirtualMachineDelegate`. It receives state change callbacks and error notifications from the framework, forwarding them into the `watch::channel`.
- `state_rx` — The receiving half of a `tokio::sync::watch` channel. Allows callers to observe state transitions reactively.
- `config` — The validated configuration used to create this VM. Retained for introspection and for state save/restore flows.

### Lifecycle Methods

```rust
impl Vm {
    /// Create a VM from a validated config.
    /// Constructs all ObjC configuration objects, creates the dispatch queue,
    /// sets up the delegate, and returns a ready-to-start Vm.
    /// Does NOT start the VM — it will be in Stopped state.
    pub async fn create(config: VmConfig) -> Result<Self>;

    /// Start the VM. Transitions: Stopped -> Starting -> Running.
    /// Returns when the VM reaches Running state.
    /// Fails if the VM is not in Stopped state.
    pub async fn start(&self) -> Result<()>;

    /// Pause the VM. Transitions: Running -> Pausing -> Paused.
    /// Returns when the VM reaches Paused state.
    /// Fails if the VM is not in Running state.
    pub async fn pause(&self) -> Result<()>;

    /// Resume a paused VM. Transitions: Paused -> Resuming -> Running.
    /// Returns when the VM reaches Running state.
    /// Fails if the VM is not in Paused state.
    pub async fn resume(&self) -> Result<()>;

    /// Request a graceful stop. Non-async — sends a stop request to the guest
    /// (equivalent to pressing the power button). The guest OS decides whether
    /// to honor it. Does NOT wait for the VM to actually stop.
    /// Use `state_stream()` to observe when it reaches Stopped.
    pub fn request_stop(&self) -> Result<()>;

    /// Force stop the VM. Transitions: any -> Stopping -> Stopped.
    /// Returns when the VM reaches Stopped state.
    /// This is an ungraceful termination — equivalent to pulling the power cord.
    pub async fn stop(&self) -> Result<()>;

    /// Save VM state to a file. The VM must be paused first.
    /// Transitions: Paused -> Saving -> Stopped (with state file on disk).
    /// The state file can be used with `restore_state` to resume later.
    pub async fn save_state(&self, path: &Path) -> Result<()>;

    /// Restore a VM from a saved state file.
    /// Creates a new Vm instance, loads the saved state, and returns it
    /// in Paused state. Call `.resume()` to continue execution.
    /// The config must match the config used when the state was saved
    /// (same CPU count, memory, devices).
    pub async fn restore_state(path: &Path, config: VmConfig) -> Result<Self>;

    /// Get the current state synchronously (non-blocking snapshot).
    pub fn state(&self) -> VmState;

    /// Get a watch receiver for state changes.
    /// The receiver yields the new VmState every time it changes.
    /// Use `changed().await` to wait for the next transition.
    pub fn state_stream(&self) -> watch::Receiver<VmState>;

    /// Connect to a vsock port inside the guest.
    /// Returns a VsockStream that implements AsyncRead + AsyncWrite.
    /// The VM must be Running and vsock must have been enabled in config.
    pub async fn vsock_connect(&self, port: u32) -> Result<VsockStream>;

    /// Listen for incoming vsock connections from the guest on the given port.
    /// Returns a VsockListener. Call `.accept()` to wait for connections.
    /// The VM must be Running and vsock must have been enabled in config.
    pub fn vsock_listen(&self, port: u32) -> Result<VsockListener>;
}
```

### State Transitions

Valid state transitions enforced by the framework (and validated by `vz` before dispatching):

```
                    ┌──────────────────────────────────────┐
                    │                                      │
                    v                                      │
Stopped ──► Starting ──► Running ──► Pausing ──► Paused   │
   ^                        │                      │  │    │
   │                        │                      │  │    │
   │                        v                      │  v    │
   │                    Stopping ──────────────────►  Saving
   │                                                  │
   │                                                  │
   └──────────────────────────────────────────────────┘

Stopped (with state file) ──► Restoring ──► Paused ──► (resume) ──► Running
```

Transition details:

| From | Action | Intermediate | To | Notes |
|------|--------|--------------|----|-------|
| Stopped | `start()` | Starting | Running | Normal boot |
| Running | `pause()` | Pausing | Paused | Guest suspended in memory |
| Paused | `resume()` | Resuming | Running | Guest continues |
| Running | `request_stop()` | — | (guest decides) | Graceful; guest may ignore |
| Running | `stop()` | Stopping | Stopped | Force kill |
| Paused | `stop()` | Stopping | Stopped | Force kill from paused |
| Paused | `save_state()` | Saving | Stopped | State written to disk |
| Stopped | `restore_state()` | Restoring | Paused | Loaded from state file |

Calling a lifecycle method from an invalid state returns `VzError::InvalidState` immediately without dispatching to ObjC.

### Thread Safety

`Vm` is `Send` but not `Sync`. It is designed to be owned by a single task but can be moved between tasks. All ObjC interactions go through the serial dispatch queue, so there are no data races internally. If shared access is needed, wrap in `Arc<Mutex<Vm>>` or use a dedicated actor task that owns the `Vm` and receives commands via a channel.

---

## VmState

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmState {
    Stopped,
    Starting,
    Running,
    Pausing,
    Paused,
    Resuming,
    Stopping,
    Saving,
    Restoring,
    Error(String),
}
```

### Mapping from VZVirtualMachineState

| VZVirtualMachineState value | VmState variant |
|-----------------------------|-----------------|
| `VZVirtualMachineStateStopped` (0) | `Stopped` |
| `VZVirtualMachineStateRunning` (1) | `Running` |
| `VZVirtualMachineStatePaused` (2) | `Paused` |
| `VZVirtualMachineStateError` (3) | `Error(description)` |
| `VZVirtualMachineStateStarting` (4) | `Starting` |
| `VZVirtualMachineStatePausing` (5) | `Pausing` |
| `VZVirtualMachineStateResuming` (6) | `Resuming` |
| `VZVirtualMachineStateStopping` (7) | `Stopping` |
| `VZVirtualMachineStateSaving` (8) | `Saving` |
| `VZVirtualMachineStateRestoring` (9) | `Restoring` |

The `Error` variant carries the localized description from the `NSError` provided by the delegate's `virtualMachine:didStopWithError:` callback.

### Observing State Changes

```rust
let vm = Vm::create(config).await?;
let mut state_rx = vm.state_stream();

// Spawn a task that reacts to state changes
tokio::spawn(async move {
    while state_rx.changed().await.is_ok() {
        let state = state_rx.borrow().clone();
        tracing::info!(?state, "VM state changed");
        if matches!(state, VmState::Error(_) | VmState::Stopped) {
            break;
        }
    }
});

vm.start().await?;
```

The `watch::channel` is initialized with `VmState::Stopped` at `Vm::create` time.

---

## VsockStream & VsockListener

Vsock (Virtio Socket) provides host-to-guest communication without networking. It uses a simple CID + port addressing scheme. The host is always CID 2, the guest is CID 3.

### VsockStream

```rust
pub struct VsockStream {
    // Internal: wraps VZVirtioSocketConnection's input/output NSStreams
    // bridged to tokio via AsyncFd or a background read/write thread.
}

impl tokio::io::AsyncRead for VsockStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>>;
}

impl tokio::io::AsyncWrite for VsockStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>;

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>;
}
```

Because `VsockStream` implements the standard tokio IO traits, it can be used with:
- `tokio::io::copy` for raw byte forwarding
- `tokio_util::codec` for framed protocols
- `tonic` for gRPC-over-vsock
- Any other tokio-compatible IO consumer

### VsockListener

```rust
pub struct VsockListener {
    // Internal: registered on VZVirtioSocketDevice for a specific port.
    // Incoming connections arrive via the device delegate and are queued
    // in an internal mpsc channel.
}

impl VsockListener {
    /// Wait for and accept the next incoming vsock connection from the guest.
    /// Returns a VsockStream for the accepted connection.
    pub async fn accept(&self) -> Result<VsockStream>;
}
```

### Connection Methods on Vm

```rust
impl Vm {
    /// Connect from host to guest on the specified vsock port.
    /// The guest must be listening on this port.
    /// Returns VzError::VsockFailed if connection is refused or times out.
    pub async fn vsock_connect(&self, port: u32) -> Result<VsockStream>;

    /// Listen for connections from the guest on the specified port.
    /// The returned VsockListener will accept connections initiated by the guest
    /// to CID 2 (host) on this port.
    pub fn vsock_listen(&self, port: u32) -> Result<VsockListener>;
}
```

### Usage Example

```rust
// Host side: listen for guest connections
let listener = vm.vsock_listen(5000)?;

tokio::spawn(async move {
    loop {
        let stream = listener.accept().await?;
        tokio::spawn(handle_connection(stream));
    }
});

// Or: host connects to guest
let stream = vm.vsock_connect(5001).await?;
let (reader, writer) = tokio::io::split(stream);
```

### Internal Bridging

`VZVirtioSocketConnection` exposes `NSInputStream` and `NSOutputStream`. These are callback-based ObjC stream objects. The bridge layer (in `bridge.rs`) converts these to file descriptors (via `CFStreamCreatePairWithSocket` or by extracting the underlying fd) and wraps them with `tokio::io::unix::AsyncFd` for non-blocking IO integration with the tokio reactor.

If the framework does not expose raw file descriptors, the fallback strategy is a pair of background threads (one for read, one for write) that perform blocking NSStream reads/writes and communicate with the async side via `tokio::sync::mpsc` channels.

---

## VirtioFS Configuration

VirtioFS shares host directories into the guest as filesystem mounts. In Virtualization.framework, these are configured via `VZVirtioFileSystemDeviceConfiguration` with a `VZSharedDirectory` source.

### Key Constraint: Mounts Are Static

VirtioFS mounts are configured at VM creation time and **cannot be added, removed, or modified while the VM is running**. This is a framework limitation, not a design choice.

Strategy for the `vz` crate: mount the workspace root directory once at VM creation, and scope per-session at the application layer (i.e., the higher-level daemon decides which subdirectory each session works in, but the mount itself covers the whole tree).

### Configuration

```rust
pub struct SharedDirConfig {
    /// Mount tag — the guest uses this to mount the filesystem.
    /// e.g., `mount -t virtiofs workspace /mnt/workspace`
    pub tag: String,

    /// Host directory to share.
    pub source: PathBuf,

    /// If true, the guest cannot write to this mount.
    pub read_only: bool,
}
```

Multiple shared directories can be configured (each with a unique tag). Inside the guest, each is mounted separately using its tag:

```bash
# Inside the guest
mount -t virtiofs workspace /mnt/workspace
mount -t virtiofs tools /mnt/tools
```

### Internal Mapping

Each `SharedDirConfig` maps to:
1. `VZSharedDirectory` — wraps the host path and read-only flag
2. `VZSingleDirectoryShare` — wraps the shared directory
3. `VZVirtioFileSystemDeviceConfiguration` — wraps the share with the tag

These ObjC objects are created inside `Vm::create()` and attached to the `VZVirtualMachineConfiguration`.

---

## macOS Installation

Creating a new macOS VM requires downloading an IPSW (firmware image) from Apple and running the installer. The `vz` crate provides a high-level async function for this.

### IpswSource

```rust
pub enum IpswSource {
    /// Download the latest compatible IPSW from Apple.
    /// Uses VZMacOSRestoreImage.fetchLatestSupportedWithCompletionHandler
    /// to discover the download URL, then streams it to a temporary file.
    Latest,

    /// Use an existing IPSW file on disk.
    /// The file is validated (correct format, compatible with this hardware).
    Path(PathBuf),
}
```

### Installation Function

```rust
/// Install macOS into a new VM disk image.
///
/// This is a long-running operation (30-60 minutes depending on disk speed
/// and network for IPSW download). Progress is reported via tracing events.
///
/// Steps:
/// 1. Create a raw disk image at `disk_path` with size `disk_size_gb`.
/// 2. Resolve the IPSW — download from Apple if `Latest`, validate if `Path`.
/// 3. Load the restore image and extract hardware model + requirements.
/// 4. Create a VZMacPlatformConfiguration with the hardware model.
/// 5. Build a minimal VZVirtualMachineConfiguration for installation.
/// 6. Create a VZMacOSInstaller and run it.
/// 7. Return when installation completes.
///
/// After installation, the disk image at `disk_path` is bootable.
/// Create a VmConfig pointing to it and call `Vm::create` + `vm.start()`.
pub async fn install_macos(
    ipsw: IpswSource,
    disk_path: &Path,
    disk_size_gb: u64,
    config: &VmConfig,
) -> Result<()>;
```

### Installation Flow

```
┌─────────────────┐
│ Create disk img  │  dd / truncate to disk_size_gb
└────────┬────────┘
         v
┌─────────────────┐
│ Resolve IPSW     │  Download from Apple (Latest) or load from path
└────────┬────────┘
         v
┌─────────────────┐
│ Extract metadata │  Hardware model, minimum CPU/memory requirements
└────────┬────────┘
         v
┌─────────────────┐
│ Build platform   │  VZMacPlatformConfiguration + hardware model +
│ config           │  machine identifier + auxiliary storage
└────────┬────────┘
         v
┌─────────────────┐
│ Create installer │  VZMacOSInstaller with the VM configuration
│ VM + run install │  and the restore image
└────────┬────────┘
         v
┌─────────────────┐
│ Wait for         │  Progress reported via tracing::info!
│ completion       │  "install progress: 45%"
└────────┬────────┘
         v
       Done — disk image is bootable
```

### Auxiliary Storage

macOS VMs require auxiliary storage (NVRAM equivalent). The `install_macos` function creates this automatically alongside the disk image at `{disk_path}.aux`. This file must be preserved — it is needed every time the VM boots.

### Hardware Model Persistence

The `VZMacHardwareModel` and `VZMacMachineIdentifier` generated during installation are serialized and stored alongside the disk image (at `{disk_path}.hwmodel` and `{disk_path}.machineid`). These are loaded automatically by `VmConfigBuilder::boot_macos()` when the disk path is provided.

---

## Error Handling

All public methods return `Result<T, VzError>`. The error type is a flat enum covering every failure category.

```rust
#[derive(Debug, thiserror::Error)]
pub enum VzError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("invalid state transition: cannot {action} while {state}")]
    InvalidState { action: String, state: String },

    #[error("VM start failed: {0}")]
    StartFailed(String),

    #[error("VM stop failed: {0}")]
    StopFailed(String),

    #[error("save state failed: {0}")]
    SaveFailed(String),

    #[error("restore state failed: {0}")]
    RestoreFailed(String),

    #[error("vsock connection failed: {0}")]
    VsockFailed(String),

    #[error("macOS installation failed: {0}")]
    InstallFailed(String),

    #[error("disk operation failed: {0}")]
    DiskError(String),

    #[error("framework error: {0}")]
    FrameworkError(String),
}
```

### Error Design Principles

- **No panics** — Every ObjC call that can fail is wrapped in error handling. `NSError` pointers are converted to `VzError::FrameworkError` with the localized description.
- **Actionable messages** — Error strings describe what went wrong and, where possible, what to do about it. Example: `InvalidConfig("cpus must be between 1 and 10 (host has 10 cores), got 24")`.
- **No `anyhow` in the public API** — `VzError` is a concrete type so consumers can match on variants. Internally, `anyhow` may be used for chaining context, but it is converted to a `VzError` variant before crossing the public API boundary.

### Result Type Alias

```rust
pub type Result<T> = std::result::Result<T, VzError>;
```

---

## Concurrency Model

The concurrency design bridges two worlds: Apple's GCD (Grand Central Dispatch) model used by Virtualization.framework, and Rust's tokio async runtime.

### Dispatch Queue

Virtualization.framework requires that all calls to `VZVirtualMachine` happen on the same serial dispatch queue that was used to create it. The `Vm` struct owns this queue and dispatches all operations to it internally.

```rust
// Internal (in bridge.rs)
pub(crate) struct DispatchQueue {
    inner: dispatch_queue_t,
}

impl DispatchQueue {
    pub fn new(label: &str) -> Self;

    /// Dispatch a closure to the queue and await its result.
    /// Bridges GCD -> tokio via a oneshot channel.
    pub async fn run<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}
```

The `run` method works by:
1. Creating a `tokio::sync::oneshot::channel`.
2. Dispatching a block to the GCD queue that executes the closure and sends the result through the channel.
3. Awaiting the oneshot receiver, which yields when the GCD block completes.

This means the tokio task that calls `vm.start()` yields to the scheduler while waiting for the ObjC operation to complete on the dispatch queue — no threads are blocked.

### Delegate (ObjC Callback Bridge)

The `VMDelegate` is defined using `objc2::define_class!` and implements `VZVirtualMachineDelegate`. It receives callbacks from the framework on the dispatch queue:

- `virtualMachine:didStopWithError:` — VM stopped due to an error
- `virtualMachine:didChangeState:` — VM state changed (custom KVO, not a real delegate method — actual implementation uses KVO on the `state` property)
- `guestDidStopVirtualMachine:` — Guest initiated shutdown

The delegate holds a `watch::Sender<VmState>` (wrapped in a thread-safe container). When a callback fires, it sends the new state through the channel. Any number of receivers can observe state changes.

```rust
// Internal (in bridge.rs)
define_class! {
    pub(crate) struct VMDelegate;

    unsafe impl ClassType for VMDelegate {
        type Super = NSObject;
    }

    // ivars
    impl VMDelegate {
        state_tx: Mutex<watch::Sender<VmState>>,
    }

    // delegate methods
    unsafe impl VZVirtualMachineDelegate for VMDelegate {
        #[method(guestDidStopVirtualMachine:)]
        fn guest_did_stop(&self, _vm: &VZVirtualMachine) {
            let _ = self.state_tx.lock().send(VmState::Stopped);
        }

        #[method(virtualMachine:didStopWithError:)]
        fn did_stop_with_error(&self, _vm: &VZVirtualMachine, error: &NSError) {
            let msg = error.localizedDescription().to_string();
            let _ = self.state_tx.lock().send(VmState::Error(msg));
        }
    }
}
```

### Summary: How a Lifecycle Call Flows

```
vm.start()                                  [tokio task, any thread]
  │
  ├─ Validate: state == Stopped             [synchronous check]
  │
  ├─ queue.run(move || {                    [dispatched to GCD serial queue]
  │      vm.startWithCompletionHandler(     [ObjC call on correct queue]
  │          |error| tx.send(error)         [completion handler]
  │      )
  │  }).await                               [tokio task yields here]
  │
  ├─ Wait for state_rx to show Running      [await on watch channel]
  │  or Error                               [delegate fires on GCD queue,
  │                                          sends through watch channel]
  │
  └─ Return Ok(()) or Err(StartFailed)      [tokio task resumes]
```

### Key Invariants

1. **All ObjC calls happen on the serial dispatch queue** — enforced by the `queue.run()` wrapper. There is no way to accidentally call a VZ method from the wrong thread.
2. **Public methods are `async` and `Send`** — safe to call from any tokio task on any thread.
3. **State is observable without polling** — `watch::channel` provides efficient push-based notification.
4. **The delegate prevents use-after-free** — it is stored as a `Retained<VMDelegate>` in the `Vm` struct, ensuring it lives at least as long as the `VZVirtualMachine` that references it.
5. **Drop behavior** — when `Vm` is dropped, the dispatch queue and delegate are released. If the VM is still running, it continues running until the ObjC runtime collects it (which may be immediately or deferred). Callers should call `stop()` before dropping if they want deterministic shutdown.
