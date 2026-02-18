# Guest Agent Design

## What It Is

A small Rust binary (`vz-guest-agent`) that runs inside the macOS VM, listens on vsock, and executes commands on behalf of the host. It is the guest-side counterpart to the host's sandbox layer — the host sends `Exec` requests, the guest agent spawns processes, streams their output back, and reports exit codes.

The guest agent is intentionally minimal. It does not interpret commands, apply policy, or make decisions. It is a remote process executor — nothing more.

## How Other Systems Do It

| System | Language | Transport | Protocol | Streaming |
|--------|----------|-----------|----------|-----------|
| Tart | Go | vsock | gRPC bidirectional | Yes |
| QEMU Guest Agent | C | virtio-serial | JSON-RPC | No (poll) |
| Cowork | Go (sdk-daemon) | vsock | ndjson | Yes |
| Kata Agent | Go/Rust | vsock | ttrpc | Unary RPCs |
| Firecracker | Rust | vsock | custom binary | N/A (no agent, mmds only) |

Key observations:

- **Tart** uses gRPC with bidirectional streaming, which gives clean streaming semantics but pulls in a heavy dependency stack (protobuf codegen, tonic, HTTP/2). Their agent (`tartlet`) runs as a launchd daemon.
- **QEMU GA** uses JSON-RPC over virtio-serial, but virtio-serial has no connection semantics and is not supported on macOS guests. Its poll-based model (host sends `guest-exec-status` repeatedly) adds latency.
- **Cowork** uses newline-delimited JSON over vsock — closest to our approach. Simple and effective.
- **Kata** uses ttrpc (lightweight gRPC variant) which is well-engineered but tightly coupled to the containerd/Kata ecosystem.
- **Firecracker** has no guest agent — it relies on the guest's init system and MMDS (Microvm Metadata Service) for configuration. Not applicable to our use case.

Our approach takes the simplicity of Cowork (JSON over vsock) with the streaming model of Tart (push-based output), without either's dependency overhead.

## Architecture

```
┌─────────────────────────────────────────────┐
│ macOS Guest VM                              │
│                                             │
│  ┌─────────────────────────────────────┐    │
│  │ vz-guest-agent                      │    │
│  │                                     │    │
│  │  vsock listener (port 7424)         │    │
│  │    │                                │    │
│  │    ├─ connection handler            │    │
│  │    │    ├─ frame reader             │    │
│  │    │    ├─ frame writer             │    │
│  │    │    └─ request dispatcher       │    │
│  │    │         ├─ Exec → spawn child  │    │
│  │    │         ├─ Signal → kill child  │    │
│  │    │         ├─ StdinWrite → pipe   │    │
│  │    │         ├─ SystemInfo → stats  │    │
│  │    │         └─ Ping → Pong        │    │
│  │    │                                │    │
│  │    └─ process table                 │    │
│  │         HashMap<u64, ChildProcess>  │    │
│  └─────────────────────────────────────┘    │
│                                             │
│  /mnt/workspace/ ← VirtioFS mount           │
└─────────────────────────────────────────────┘
```

- **Single binary**: `vz-guest-agent`, statically linked, no external dependencies at runtime.
- **Listens on vsock port 7424**: hardcoded default, overridable via command-line flag.
- **Spawns child processes** with `tokio::process::Command`.
- **Hooks stdout/stderr** via async pipes, forwarding each chunk as a `Response::Stdout` or `Response::Stderr` frame.
- **Manages process lifecycle**: stdin forwarding, signal delivery (SIGTERM, SIGKILL, etc.), exit code collection.

## Bootstrap

The guest agent is baked into the golden VM image and managed by launchd.

### Installation path

```
/usr/local/bin/vz-guest-agent
```

### Launchd plist

`/Library/LaunchDaemons/com.vz.guest-agent.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.vz.guest-agent</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/vz-guest-agent</string>
        <string>--port</string>
        <string>7424</string>
    </array>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>/var/log/vz-guest-agent.stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/var/log/vz-guest-agent.stderr.log</string>

    <key>ThrottleInterval</key>
    <integer>5</integer>

    <key>ProcessType</key>
    <string>Interactive</string>

    <key>Nice</key>
    <integer>-5</integer>
</dict>
</plist>
```

Key properties:

- **RunAtLoad: true** — starts automatically at boot, no user login required.
- **KeepAlive: true** — launchd restarts the agent if it crashes.
- **ThrottleInterval: 5** — if the agent crashes repeatedly, launchd waits at least 5 seconds between restarts (prevents tight crash loops).
- **ProcessType: Interactive** — tells macOS to give the agent higher scheduling priority.
- **Nice: -5** — slightly elevated priority since the agent is on the critical path for all command execution.
- **Runs as root** — simplicity. The agent needs to execute arbitrary commands that may require elevated privileges (installing packages, modifying system files). A future iteration could split into a privileged daemon and an unprivileged agent (like Tart's tartlet/tarteletd split), but for now root is sufficient.

## Guest Agent Protocol

The protocol is the same as defined in `03-vsock-protocol.md`, viewed from the guest's perspective.

### Listening

```rust
use std::os::unix::io::RawFd;
use libc::{AF_VSOCK, SOCK_STREAM, VMADDR_CID_ANY};

fn listen_vsock(port: u32) -> Result<RawFd> {
    let fd = unsafe { libc::socket(AF_VSOCK, SOCK_STREAM, 0) };

    let addr = libc::sockaddr_vm {
        svm_len: std::mem::size_of::<libc::sockaddr_vm>() as u8,
        svm_family: AF_VSOCK as u8,
        svm_reserved1: 0,
        svm_port: port,
        svm_cid: VMADDR_CID_ANY,  // accept from any CID (the host)
    };

    unsafe {
        libc::bind(fd, &addr as *const _ as *const libc::sockaddr,
                   std::mem::size_of::<libc::sockaddr_vm>() as u32);
        libc::listen(fd, 1);
    }

    Ok(fd)
}
```

### Connection handling

```rust
async fn handle_connection(stream: VsockStream) {
    let (reader, writer) = stream.split();
    let writer = Arc::new(Mutex::new(writer));
    let processes: Arc<Mutex<HashMap<u64, Child>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let request = read_frame(&mut reader).await?;
        match request {
            Request::Exec { id, command, args, working_dir, env } => {
                spawn_process(id, command, args, working_dir, env,
                             writer.clone(), processes.clone()).await?;
            }
            Request::Signal { exec_id, signal } => {
                signal_process(exec_id, signal, &processes).await?;
            }
            Request::StdinWrite { id, exec_id, data } => {
                write_stdin(exec_id, &data, &processes).await?;
                send_frame(&writer, Response::Ok { id }).await?;
            }
            Request::StdinClose { exec_id } => {
                close_stdin(exec_id, &processes).await?;
            }
            Request::Ping { id } => {
                send_frame(&writer, Response::Pong { id }).await?;
            }
            Request::SystemInfo { id } => {
                let info = collect_system_info().await?;
                send_frame(&writer, Response::SystemInfoResult { id, ..info }).await?;
            }
            Request::ResourceStats { id } => {
                let stats = collect_resource_stats().await?;
                send_frame(&writer, Response::ResourceStatsResult { id, ..stats }).await?;
            }
        }
    }
}
```

### Dispatching Exec requests

When an `Exec` request arrives:

1. Spawn the process with `tokio::process::Command`, piping stdout and stderr.
2. Store the `Child` handle in the process table keyed by `exec_id`.
3. Spawn two async tasks: one reading stdout, one reading stderr.
4. Each task reads chunks from the pipe and sends `Response::Stdout` / `Response::Stderr` frames.
5. When the process exits, send `Response::ExitCode` and remove from the process table.

### Handling Signal

Forward the signal to the child process:

```rust
async fn signal_process(exec_id: u64, signal: i32, processes: &ProcessTable) {
    if let Some(child) = processes.lock().await.get(&exec_id) {
        unsafe { libc::kill(child.id() as i32, signal); }
    }
}
```

### Handling StdinWrite / StdinClose

Forward data to the child's stdin pipe:

```rust
async fn write_stdin(exec_id: u64, data: &[u8], processes: &ProcessTable) {
    if let Some(child) = processes.lock().await.get_mut(&exec_id) {
        if let Some(stdin) = child.stdin.as_mut() {
            stdin.write_all(data).await?;
        }
    }
}
```

### Handling Ping

Immediately respond with `Pong`. Used by the host to verify the agent is alive and responsive before sending real commands.

### Handling SystemInfo

Collect system statistics and return:

```rust
async fn collect_system_info() -> Result<SystemInfo> {
    // CPU count
    let cpu_count = num_cpus::get() as u32;

    // Total memory (sysctl hw.memsize on macOS)
    let memory_bytes = sysctl_hw_memsize()?;

    // Disk free (statfs on /mnt/workspace)
    let disk_free_bytes = statfs_free("/mnt/workspace")?;

    // OS version (sw_vers)
    let os_version = read_sw_vers()?;

    Ok(SystemInfo { cpu_count, memory_bytes, disk_free_bytes, os_version })
}
```

## Process Management

### Process table

```rust
struct ProcessEntry {
    child: tokio::process::Child,
    started_at: Instant,
}

type ProcessTable = Arc<Mutex<HashMap<u64, ProcessEntry>>>;
```

Each `Exec` request creates an entry. The entry is removed when:
- The process exits naturally (ExitCode sent)
- The process is killed via Signal
- The connection drops (all children are killed on disconnect)

### Concurrent processes

Multiple exec_ids can be active simultaneously on one connection. The process table is the multiplexing point — each stdout/stderr reader task tags its frames with the correct `exec_id`, and the host demultiplexes on the other side.

### Resource limits

The guest agent does not enforce resource limits on child processes. Resource constraints (CPU, memory) are applied at the VM level via `VZVirtualMachineConfiguration`. The agent trusts that the host has configured appropriate VM-level limits.

## Connection Lifecycle

### Startup

1. launchd starts `vz-guest-agent` at boot.
2. Agent creates vsock listener on port 7424.
3. Agent enters accept loop, waiting for host connections.

### Normal operation

1. Host calls `VZVirtioSocketDevice.connect(toPort: 7424)`.
2. Guest agent accepts the connection.
3. Host sends requests, guest agent processes them and sends responses.
4. Connection persists for the lifetime of the session.

### Disconnection

When the vsock connection drops (host disconnects, VM state restore, etc.):

1. Agent detects EOF/error on the connection.
2. Agent kills all child processes spawned on that connection (SIGTERM, then SIGKILL after 5s).
3. Agent cleans up the process table.
4. Agent returns to the accept loop, ready for the next connection.

The agent itself **never stops** — only the connection drops. This means:
- The host can reconnect without restarting the guest.
- VM saved-state restore works: the agent is still running, the host just needs to reconnect.
- Crash recovery: if the host process crashes and restarts, it can reconnect to the same agent.

### Multiple connections

The agent accepts one connection at a time. If a second connection arrives while the first is active, it is queued until the first disconnects. This simplifies the agent's state management and matches the 1:1 host-to-VM relationship.

## Cross-Compilation

The guest agent runs on macOS (aarch64) inside an Apple Silicon VM. Since the host is also macOS on Apple Silicon:

```bash
cargo build --release -p vz-guest-agent --target aarch64-apple-darwin
```

No cross-compilation is needed — same architecture, same OS. The resulting binary is a plain Rust executable with no ObjC dependencies, no framework linkage, and no dynamic libraries beyond system libc.

The built binary is copied into the golden VM image during image preparation:

```bash
# From the host, copy into the mounted guest disk image
cp target/release/vz-guest-agent /Volumes/GuestDisk/usr/local/bin/vz-guest-agent
```

## Non-Root Command Execution

### The Problem

The guest agent runs as root (via launchd). By default, all `Exec` requests run child processes as root. But most agent workloads should run as the unprivileged `dev` user — compiling code, running tests, editing files. Running everything as root is unnecessarily risky: a buggy agent command could modify system files, install rootkits, or corrupt the macOS installation.

### User Field on Exec

The `Exec` request includes an optional `user` field:

```rust
Exec {
    id: u64,
    command: String,
    args: Vec<String>,
    working_dir: Option<String>,
    env: Vec<(String, String)>,
    user: Option<String>,  // NEW: run as this user
}
```

- If `user` is `None`: run as the agent's user (root). Backwards-compatible with protocol v1.
- If `user` is `Some("dev")`: run the command as the `dev` user.

### Implementation

On macOS, the agent uses `launchctl asuser` to run the command as the specified user:

```rust
async fn spawn_as_user(user: &str, command: &str, args: &[String], working_dir: &str) -> Result<Child> {
    // Get the UID for the username
    let uid = get_uid_for_user(user)?;

    // Use launchctl asuser to run in the user's context
    // This sets up the correct Mach bootstrap namespace, which matters
    // for Xcode toolchain, Homebrew, and other user-scoped services.
    let child = tokio::process::Command::new("launchctl")
        .arg("asuser")
        .arg(uid.to_string())
        .arg(command)
        .args(args)
        .current_dir(working_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::piped())
        .spawn()?;

    Ok(child)
}
```

Why `launchctl asuser` instead of `su` or `sudo -u`:
- `launchctl asuser` runs the command in the correct Mach bootstrap namespace for the user. This is important on macOS because many developer tools (Xcode, Homebrew, `security` keychain access) depend on the user's bootstrap context.
- `su` and `sudo -u` change the Unix UID but don't set up the Mach context, which causes subtle failures with macOS-specific tooling.

### Capability Gating

The `user` field requires the `"user_exec"` capability announced during the handshake. If the host sends an `Exec` with `user` set and the guest agent doesn't support it, the agent responds with `ExecError` indicating the capability is missing.

### Default in vz-sandbox

The `SandboxSession` sets `user: Some("dev")` by default. Root execution is available but opt-in:

```rust
impl SandboxSession {
    /// Execute as the default user (dev)
    pub async fn exec(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecOutput>;

    /// Execute as root
    pub async fn exec_as_root(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecOutput>;
}
```

## Vsock Security Model

### Attack Surface

The guest agent listens on vsock port 7424. Any process inside the guest can connect to the host via vsock (CID 2). The security question: what happens if a compromised process inside the guest connects to the host?

### Host-Side Validation

The host should only accept vsock connections from expected guest ports. The `vz` crate's `VsockListener` is used by the host to listen for guest-initiated connections. The host must:

1. **Only listen on expected ports** — Don't create a VsockListener unless needed.
2. **Validate the source** — `VZVirtioSocketConnection` provides the source port. Reject connections from unexpected ports.
3. **Never expose sensitive operations on vsock** — The host-to-guest channel is for command execution. Any host-side vsock listener should be read-only or tightly scoped.

### Guest-to-Host Direction

In the standard vz architecture, communication is **host-initiated only**: the host connects to the guest agent on port 7424. The guest agent does not initiate connections to the host.

If a compromised process inside the guest attempts to connect to the host on CID 2:
- There is no listener on the host side (unless the consumer explicitly creates one).
- The connection is refused.
- The VM's vsock device does not provide a general-purpose channel to the host — only connections to ports where the host has registered a `VZVirtioSocketListener`.

This is inherently secure: the guest cannot reach the host unless the host explicitly opens a listener.

### Recommendations for Consumers

1. Do not create host-side vsock listeners unless you have a specific use case.
2. If you do listen for guest-initiated connections, treat all data as untrusted.
3. The guest agent should be treated as a minimal, auditable component — resist adding features to it that could be exploited (file upload, arbitrary file read, etc.).

## Connection Reconnect and Draining

### The Race Condition

The agent accepts one connection at a time. When the host disconnects and immediately reconnects, there is a window where:
- The old connection's cleanup (killing child processes) is still running
- The new connection's `Exec` requests begin executing

This could cause the new session's processes to be killed by the old connection's cleanup.

### Solution: Connection Draining

When the agent detects a disconnect:

1. Enter a "draining" state for the old connection.
2. Kill all child processes associated with the old connection (SIGTERM → 5s → SIGKILL).
3. Wait until all processes have exited and the process table is empty.
4. Only then accept the next connection.

If a new connection arrives during draining, it is queued in the kernel's `listen` backlog (backlog size 1). The `accept` call returns it once draining completes.

### Implementation

```rust
async fn accept_loop(listener: VsockListener) {
    loop {
        let stream = listener.accept().await?;
        let result = handle_connection(stream).await;

        // Connection ended (EOF, error, or host disconnect).
        // Drain: kill all child processes before accepting next connection.
        drain_processes(&process_table).await;

        // Now safe to accept next connection.
        tracing::info!("connection drained, ready for next");
    }
}

async fn drain_processes(table: &ProcessTable) {
    let mut processes = table.lock().await;

    // SIGTERM all children
    for (id, entry) in processes.iter() {
        unsafe { libc::kill(entry.child.id() as i32, libc::SIGTERM); }
    }

    // Wait up to 5 seconds for graceful exit
    let deadline = Instant::now() + Duration::from_secs(5);
    while !processes.is_empty() && Instant::now() < deadline {
        // Check for exited processes
        let mut exited = vec![];
        for (id, entry) in processes.iter_mut() {
            if let Ok(Some(_)) = entry.child.try_wait() {
                exited.push(*id);
            }
        }
        for id in exited {
            processes.remove(&id);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // SIGKILL any remaining
    for (id, entry) in processes.iter() {
        unsafe { libc::kill(entry.child.id() as i32, libc::SIGKILL); }
    }
    processes.clear();
}
```

This ensures a clean handoff between sessions with zero risk of cross-session process leakage.
