# vz-sandbox Pool & Session Design

## Purpose

`vz-sandbox` is a high-level abstraction that provides a "mount a folder, run commands, tear down" interface on top of the lower-level VM, VirtioFS, and vsock primitives. It hides the complexity of VM lifecycle, vsock connection management, and process multiplexing behind a simple pool-based API.

The typical usage pattern:

1. Create a pool of warm VMs at startup.
2. Acquire a session scoped to a project directory.
3. Execute commands in the sandbox.
4. Release the session back to the pool.

## Module Structure

```
vz-sandbox/src/
├── lib.rs          # Public API exports
├── pool.rs         # SandboxPool
├── session.rs      # SandboxSession, ExecOutput
├── channel.rs      # Channel<Req, Resp> typed protocol
└── config.rs       # SandboxConfig
```

### lib.rs

Re-exports the public API surface:

```rust
pub use config::SandboxConfig;
pub use pool::SandboxPool;
pub use session::{SandboxSession, ExecOutput, ExecStream, ExecEvent};
pub use channel::Channel;
```

### config.rs

```rust
pub struct SandboxConfig {
    /// Path to the golden disk image (e.g., ~/.vz/images/base.img)
    pub image_path: PathBuf,

    /// CPU cores allocated to each VM (default: 4)
    pub cpus: u8,

    /// Memory in GB allocated to each VM (default: 8)
    pub memory_gb: u64,

    /// Path to a saved VM state for fast restore (skip full boot)
    /// If None, VMs cold-boot from the disk image
    pub state_path: Option<PathBuf>,

    /// Host workspace root directory, mounted into the guest via VirtioFS
    /// All project_dir paths must be under this root
    pub workspace_mount: PathBuf,

    /// vsock port where the guest agent listens (default: 7424)
    pub agent_port: u32,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            image_path: PathBuf::new(), // must be set
            cpus: 4,       // Higher than vz crate default (2) — sandbox workloads need more
            memory_gb: 8,  // Higher than vz crate default (4) — compilation is memory-hungry
            state_path: None,
            workspace_mount: PathBuf::new(), // must be set
            agent_port: 7424,
        }
    }
}
```

## SandboxPool

```rust
pub struct SandboxPool {
    config: SandboxConfig,
    vms: Mutex<Vec<PoolEntry>>,
}

struct PoolEntry {
    vm: Vm,
    in_use: bool,
}
```

### API

```rust
impl SandboxPool {
    /// Create a pool of VMs. pool_size is clamped to a maximum of 2.
    pub async fn new(config: SandboxConfig, pool_size: u8) -> Result<Self>;

    /// Acquire a VM from the pool, scoped to the given project directory.
    /// project_dir must be a subdirectory of config.workspace_mount.
    pub async fn acquire(&self, project_dir: &Path) -> Result<SandboxSession>;

    /// Release a session back to the pool.
    pub async fn release(&self, session: SandboxSession) -> Result<()>;
}
```

### Pool Behavior

**Pool size limit**: `pool_size` is clamped to **2**. macOS has a kernel-level limit on the number of concurrent macOS guest VMs that can run on Apple Silicon. Attempting to create more than 2 macOS guests results in a Virtualization.framework error. The pool enforces this limit at construction time.

**Construction (`new`)**:

1. Clamp `pool_size` to `min(pool_size, 2)`.
2. For each VM slot:
   a. Create a `VmConfig` from the `SandboxConfig` (image path, CPU, memory, VirtioFS mount).
   b. Create the `Vm` instance.
   c. If `state_path` is `Some`, restore from saved state (fast path, ~1-2 seconds).
   d. Otherwise, cold boot the VM (slow path, ~30-60 seconds).
   e. Wait for the guest agent to become reachable (send `Ping`, wait for `Pong`).
3. Return the pool with all VMs warm and ready.

**Acquisition (`acquire`)**:

1. Validate that `project_dir` is a subdirectory of `config.workspace_mount`. If not, return an error. This prevents accessing directories outside the mounted workspace.
2. Find a `PoolEntry` where `in_use == false`. If none available, return an error (pool exhausted).
3. Mark the entry as `in_use = true`.
4. Connect to the guest agent over vsock at `config.agent_port`.
5. Compute the guest-side mount path: `/mnt/workspace/<relative_path>` where `relative_path = project_dir.strip_prefix(workspace_mount)`.
6. Return a `SandboxSession` with the VM, agent connection, and project path.

**Release (`release`)**:

1. Kill any remaining child processes in the guest (send `Signal` for all active exec_ids).
2. Disconnect from the guest agent.
3. Mark the `PoolEntry` as `in_use = false`.
4. The VM stays running — no reboot, no state reset. This is intentional: the workspace mount is shared filesystem state anyway, and the agent cleans up processes on disconnect.

### Error Recovery

- If the guest agent is unreachable during `acquire`, attempt to reconnect up to 3 times with exponential backoff (1s, 2s, 4s).
- If the VM itself is unresponsive (agent unreachable after retries), mark the pool entry as poisoned and attempt to restart the VM.
- A poisoned entry is replaced with a fresh VM on next `acquire` attempt.

## SandboxSession

```rust
pub struct SandboxSession {
    vm: Vm,
    agent: AgentConnection,
    project_dir: PathBuf,
    guest_project_path: String,
    next_id: AtomicU64,
}
```

### API

```rust
impl SandboxSession {
    /// Execute a command and wait for completion. Returns collected output.
    /// Timeout is optional — falls back to SandboxConfig.default_exec_timeout if None.
    pub async fn exec(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecOutput>;

    /// Execute a command and return a stream of output events.
    /// If timeout is set, the stream will error after the duration.
    pub async fn exec_streaming(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecStream>;

    /// Execute a command as root (bypasses default user: "dev").
    pub async fn exec_as_root(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecOutput>;

    /// The guest-side path to the project directory (e.g., "/mnt/workspace/my-project").
    pub fn project_path(&self) -> &str;
}
```

### exec (blocking)

```rust
pub async fn exec(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecOutput> {
    let id = self.next_id.fetch_add(1, Ordering::SeqCst);
    let timeout = timeout.or(self.config.default_exec_timeout);

    // Parse command into program + args (shell-split)
    let parts = shell_words::split(cmd)?;
    let (command, args) = parts.split_first().context("empty command")?;

    // Send Exec request (default: run as "dev" user, not root)
    self.agent.send(Request::Exec {
        id,
        command: command.to_string(),
        args: args.iter().map(|s| s.to_string()).collect(),
        working_dir: Some(self.guest_project_path.clone()),
        env: vec![],
        user: Some("dev".to_string()),
    }).await?;

    // Collect output until ExitCode
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    loop {
        let response = self.agent.recv().await?;
        match response {
            Response::Stdout { exec_id, data } if exec_id == id => {
                stdout.extend_from_slice(&data);
            }
            Response::Stderr { exec_id, data } if exec_id == id => {
                stderr.extend_from_slice(&data);
            }
            Response::ExitCode { exec_id, code } if exec_id == id => {
                return Ok(ExecOutput {
                    exit_code: code,
                    stdout: String::from_utf8_lossy(&stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&stderr).into_owned(),
                });
            }
            Response::ExecError { id: err_id, error } if err_id == id => {
                return Err(anyhow::anyhow!("exec failed: {}", error));
            }
            _ => {} // ignore responses for other exec_ids
        }
    }
}
```

### exec_streaming (async stream)

```rust
pub async fn exec_streaming(&self, cmd: &str) -> Result<ExecStream> {
    let id = self.next_id.fetch_add(1, Ordering::SeqCst);

    // Send Exec request (same parsing as exec)
    // ...

    // Return a stream that yields ExecEvents
    Ok(ExecStream::new(id, self.agent.clone()))
}
```

The caller consumes the stream:

```rust
let mut stream = session.exec_streaming("cargo build").await?;
while let Some(event) = stream.next().await {
    match event? {
        ExecEvent::Stdout(data) => print!("{}", String::from_utf8_lossy(&data)),
        ExecEvent::Stderr(data) => eprint!("{}", String::from_utf8_lossy(&data)),
        ExecEvent::Exit(code) => println!("Process exited with code {}", code),
    }
}
```

## ExecOutput & ExecStream

```rust
/// Collected output from a completed command.
pub struct ExecOutput {
    /// Process exit code. 0 = success.
    pub exit_code: i32,

    /// Captured stdout as a string.
    pub stdout: String,

    /// Captured stderr as a string.
    pub stderr: String,
}

/// Async stream of events from a running command.
pub struct ExecStream {
    exec_id: u64,
    agent: AgentConnection,
    finished: bool,
}

impl ExecStream {
    /// Get the next event. Returns None when the process has exited.
    pub async fn next(&mut self) -> Option<Result<ExecEvent>> {
        if self.finished {
            return None;
        }
        loop {
            let response = self.agent.recv().await;
            match response {
                Ok(Response::Stdout { exec_id, data }) if exec_id == self.exec_id => {
                    return Some(Ok(ExecEvent::Stdout(data)));
                }
                Ok(Response::Stderr { exec_id, data }) if exec_id == self.exec_id => {
                    return Some(Ok(ExecEvent::Stderr(data)));
                }
                Ok(Response::ExitCode { exec_id, code }) if exec_id == self.exec_id => {
                    self.finished = true;
                    return Some(Ok(ExecEvent::Exit(code)));
                }
                Ok(Response::ExecError { id, error }) if id == self.exec_id => {
                    self.finished = true;
                    return Some(Err(anyhow::anyhow!("exec failed: {}", error)));
                }
                Ok(_) => continue, // skip responses for other exec_ids
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// A single event from a running command.
pub enum ExecEvent {
    /// Chunk of stdout data.
    Stdout(Vec<u8>),

    /// Chunk of stderr data.
    Stderr(Vec<u8>),

    /// Process exited with the given code.
    Exit(i32),
}
```

## Channel<Req, Resp>

A generic typed protocol channel for custom host-to-guest communication beyond the built-in Exec/Signal/Ping commands. This enables extending the guest agent with custom request/response types without modifying the core protocol.

```rust
use serde::{Serialize, de::DeserializeOwned};

pub struct Channel<Req, Resp> {
    stream: VsockStream,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Serialize, Resp: DeserializeOwned> Channel<Req, Resp> {
    /// Create a channel over a vsock connection.
    pub fn new(stream: VsockStream) -> Self {
        Self {
            stream,
            _phantom: PhantomData,
        }
    }

    /// Send a request without waiting for a response.
    pub async fn send(&self, req: Req) -> Result<()> {
        let json = serde_json::to_vec(&req)?;
        let len = (json.len() as u32).to_le_bytes();
        self.stream.write_all(&len).await?;
        self.stream.write_all(&json).await?;
        Ok(())
    }

    /// Receive a response.
    pub async fn recv(&self) -> Result<Resp> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;

        if len > 16 * 1024 * 1024 {
            return Err(anyhow::anyhow!("frame too large: {} bytes", len));
        }

        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await?;
        let resp: Resp = serde_json::from_slice(&payload)?;
        Ok(resp)
    }

    /// Send a request and wait for the response (request-response pattern).
    pub async fn request(&self, req: Req) -> Result<Resp> {
        self.send(req).await?;
        self.recv().await
    }
}
```

Built on the same length-prefixed JSON wire format defined in `03-vsock-protocol.md`. The `Channel` type is generic over request and response types, so it can be used for the built-in protocol (`Channel<Request, Response>`) or for custom extensions on different vsock ports.

## Session Lifecycle

The complete lifecycle from pool creation to session teardown:

### 1. Pool initialization

```rust
let config = SandboxConfig {
    image_path: PathBuf::from("/Users/dev/.vz/images/base.img"),
    cpus: 4,
    memory_gb: 8,
    state_path: Some(PathBuf::from("/Users/dev/.vz/states/base.state")),
    workspace_mount: PathBuf::from("/Users/dev/workspace"),
    agent_port: 7424,
};

let pool = SandboxPool::new(config, 2).await?;
// Two VMs are now running, restored from saved state, guest agents reachable.
```

### 2. Acquire a session

```rust
let session = pool.acquire(Path::new("/Users/dev/workspace/my-project")).await?;
// VM assigned, vsock connected, project path validated.
// session.project_path() == "/mnt/workspace/my-project"
```

### 3. Execute commands

```rust
// Blocking execution
let output = session.exec("cargo build --release").await?;
if output.exit_code != 0 {
    eprintln!("Build failed:\n{}", output.stderr);
}

// Streaming execution
let mut stream = session.exec_streaming("cargo test").await?;
while let Some(event) = stream.next().await {
    match event? {
        ExecEvent::Stdout(data) => io::stdout().write_all(&data)?,
        ExecEvent::Stderr(data) => io::stderr().write_all(&data)?,
        ExecEvent::Exit(code) => println!("\nTests exited with code {}", code),
    }
}
```

### 4. Release the session

```rust
pool.release(session).await?;
// Child processes killed, agent connection closed, VM returned to pool.
// VM is still running and can be acquired again immediately.
```

### 5. Shutdown

```rust
drop(pool);
// All VMs are stopped. Saved state is preserved if configured.
```

### Typical integration with HQ

```rust
// In HQ's worker backend
let session = sandbox_pool.acquire(&project_path).await?;

// Run agent session commands
let result = session.exec("opencode run --prompt 'fix the build'").await?;

// Release back to pool when session completes
sandbox_pool.release(session).await?;
```

The sandbox pool replaces Docker as the worker backend — instead of creating containers, HQ acquires sandbox sessions from the pool. The guest VM runs macOS natively with full Xcode/Swift/system framework support, which Docker on macOS cannot provide.

## Session Isolation

### The Problem

The pool reuses VMs across sessions without rebooting. If session A installs malware, modifies system binaries, or plants a credential harvester, session B inherits all of it. For a sandbox library designed to run untrusted agent code, this is a security gap.

### Isolation Modes

`SandboxConfig` supports two isolation modes. The consumer chooses based on their security requirements.

```rust
pub enum IsolationMode {
    /// Fast: VM stays running between sessions. No filesystem reset.
    /// Use when sessions are trusted or when speed matters more than isolation.
    /// ~0ms acquire time (just reconnect to guest agent).
    Reuse,

    /// Secure: VM is restored from saved state between sessions.
    /// Every session starts from a clean, known-good snapshot.
    /// Use when running untrusted agent code.
    /// ~5-10s acquire time (restore from saved state).
    RestoreOnAcquire,
}
```

Added to `SandboxConfig`:

```rust
pub struct SandboxConfig {
    // ... existing fields ...

    /// How to isolate sessions from each other (default: RestoreOnAcquire)
    pub isolation: IsolationMode,
}
```

### RestoreOnAcquire Flow

When `isolation == RestoreOnAcquire`:

1. **acquire()** — Instead of reusing the running VM:
   a. Stop the current VM (force stop, fast — not graceful).
   b. Restore from `state_path` (the clean saved state with guest agent running).
   c. Wait for guest agent to respond to `Ping`.
   d. Connect and return the session.

2. **release()** — Same as before (kill child processes, disconnect).

The saved state file is created once during `vz init` (after golden image is provisioned and guest agent is confirmed running). It represents the "clean room" — no user data, no session artifacts, just a booted macOS with the guest agent listening.

### Why Not APFS Snapshots

macOS supports APFS snapshots (`tmutil localsnapshot`), which could theoretically provide filesystem rollback without a full VM restart. However:

- APFS snapshots are copy-on-write at the block level — rolling back a snapshot does not kill running processes or reset in-memory state.
- A compromised guest agent or running malware survives an APFS rollback.
- Snapshot management inside the guest requires root access and coordination with the host.
- VM state restore is simpler, more complete (resets CPU, memory, disk, and all process state), and already supported by the `vz` crate.

VM state restore is the right tool for this job.

### Default

The default isolation mode is `RestoreOnAcquire`. The 5-10s overhead is acceptable for the sandbox use case (agent sessions run for minutes to hours). Consumers who need faster turnover can opt into `Reuse` with full awareness of the security tradeoff.

## Exec Timeouts

### Per-Exec Timeout

The `exec` and `exec_streaming` methods accept an optional timeout:

```rust
impl SandboxSession {
    /// Execute a command with a timeout. Returns ExecOutput or times out.
    pub async fn exec(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecOutput>;

    /// Execute a command and return a stream of output events.
    /// If timeout is set, the stream will error after the duration.
    pub async fn exec_streaming(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecStream>;
}
```

When a timeout fires:
1. Send `Signal { exec_id, signal: SIGTERM }` to the guest agent.
2. Wait 5 seconds for the process to exit.
3. If still running, send `Signal { exec_id, signal: SIGKILL }`.
4. Return `SandboxError::ExecTimeout` with whatever stdout/stderr was collected.

### SandboxError

The `vz-sandbox` crate defines its own error type (separate from `vz::VzError`):

```rust
#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    #[error("exec timed out after {0:?}")]
    ExecTimeout(Duration),

    #[error("pool exhausted: all VMs are in use")]
    PoolExhausted,

    #[error("project dir {0} is not under workspace mount {1}")]
    ProjectOutsideWorkspace(PathBuf, PathBuf),

    #[error("guest agent unreachable after {attempts} attempts")]
    AgentUnreachable { attempts: u32 },

    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    #[error(transparent)]
    Vm(#[from] vz::VzError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
```

### Session-Level Timeout

`SandboxConfig` supports a default timeout applied to all exec calls in a session:

```rust
pub struct SandboxConfig {
    // ... existing fields ...

    /// Default timeout for exec calls. None = no timeout.
    /// Can be overridden per-exec.
    pub default_exec_timeout: Option<Duration>,
}
```

### Timeout Hierarchy

1. Per-exec timeout (if provided) takes precedence.
2. Session-level `default_exec_timeout` applies if no per-exec timeout.
3. If neither is set, exec runs indefinitely (caller's responsibility to manage).

## Network Isolation

### The Problem

By default, VMs created with `VmConfigBuilder` get NAT networking. This means an agent running inside the sandbox can make arbitrary outbound connections — exfiltrating data, downloading malware, or reaching internal services.

For a security-focused sandbox, the default should be restrictive.

### Network Policy in SandboxConfig

```rust
pub enum NetworkPolicy {
    /// No network device attached to the VM.
    /// The guest has no network interface at all.
    /// Use for maximum isolation when network access is not needed.
    None,

    /// NAT networking — guest can reach the internet via host's network.
    /// No inbound connections from the network to the guest.
    Nat,
}
```

Added to `SandboxConfig`:

```rust
pub struct SandboxConfig {
    // ... existing fields ...

    /// Network policy for sandbox VMs (default: None)
    pub network: NetworkPolicy,
}
```

### Default: No Network

The sandbox layer defaults to `NetworkPolicy::None`. This is the secure default — an agent that needs network access must explicitly opt in.

This is distinct from the `vz` crate's `VmConfigBuilder`, which defaults to NAT. The `vz` crate is a general-purpose VM library; the sandbox layer is security-first. Different defaults for different abstraction levels.

### Future: Allowlist-Based Filtering

A future `NetworkPolicy::Filtered` variant could provide allowlist-based outbound filtering:

```rust
NetworkPolicy::Filtered {
    allow: vec![
        "crates.io",
        "registry.npmjs.org",
        "github.com",
        "*.githubusercontent.com",
    ],
}
```

This would require a transparent proxy or DNS-based filtering inside the guest. Not in scope for the initial implementation, but the enum is designed to be extended.
