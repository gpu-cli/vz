# 03 — Container Lifecycle & Runtime API

## Overview

The `vz-oci` crate provides the top-level API for running OCI containers in hardware-isolated VMs. It combines image management (from `02-oci-images.md`) with VM lifecycle (from `vz` + `vz-linux`) into a clean container abstraction.

## Module Structure

```
vz-oci/src/
├── lib.rs            # Public API exports
├── runtime.rs        # Runtime: the top-level entry point
├── container.rs      # Container: a running or stopped container
├── config.rs         # RunConfig, Mount, PortMapping, RuntimeConfig
├── image.rs          # ImageStore, ImagePuller, ImageId
├── network.rs        # Port forwarding, network configuration
└── store.rs          # Container metadata storage
```

## RuntimeConfig

```rust
pub struct RuntimeConfig {
    /// Path to Linux kernel (default: ~/.vz/linux/vmlinux)
    pub kernel_path: PathBuf,

    /// Path to initramfs (default: ~/.vz/linux/initramfs.img)
    pub initramfs_path: PathBuf,

    /// OCI data directory (default: ~/.vz/oci/)
    pub oci_dir: PathBuf,

    /// Default CPU cores per container (default: 2)
    pub default_cpus: u8,

    /// Default memory per container in MB (default: 512)
    pub default_memory_mb: u64,

    /// Registry authentication
    pub auth: Auth,
}
```

## Runtime

```rust
pub struct Runtime {
    config: RuntimeConfig,
    image_store: ImageStore,
    puller: ImagePuller,
    containers: Mutex<HashMap<String, ContainerState>>,
}

impl Runtime {
    /// Create a new runtime.
    pub async fn new(config: RuntimeConfig) -> Result<Self>;

    /// Pull an image from a registry. No-op if already cached.
    pub async fn pull(&self, reference: &str) -> Result<ImageId>;

    /// Create and start a container from an image.
    /// Pulls the image if not cached.
    pub async fn run(&self, image: &str, config: RunConfig) -> Result<Container>;

    /// Create a container without starting it.
    pub async fn create(&self, image: &str, config: RunConfig) -> Result<Container>;

    /// List all containers (running and stopped).
    pub async fn list(&self) -> Vec<ContainerInfo>;

    /// Get a container by ID.
    pub async fn get(&self, id: &str) -> Option<Container>;

    /// Remove a stopped container. Cleans up rootfs.
    pub async fn remove(&self, id: &str) -> Result<()>;

    /// List cached images.
    pub async fn images(&self) -> Vec<ImageInfo>;

    /// Remove unused images and layers.
    pub async fn prune_images(&self) -> Result<PruneResult>;
}
```

## RunConfig

```rust
pub struct RunConfig {
    /// Command to run (overrides image entrypoint + cmd)
    pub cmd: Vec<String>,

    /// Environment variables (merged with image defaults)
    pub env: Vec<(String, String)>,

    /// Working directory (overrides image default)
    pub working_dir: Option<String>,

    /// User to run as (overrides image default)
    pub user: Option<String>,

    /// Bind mounts: (host_path, guest_path)
    pub mounts: Vec<Mount>,

    /// Port mappings: host_port → container_port
    pub ports: Vec<PortMapping>,

    /// CPU cores for this container (overrides runtime default)
    pub cpus: Option<u8>,

    /// Memory in MB for this container (overrides runtime default)
    pub memory_mb: Option<u64>,

    /// Enable network access (default: true for Linux containers)
    pub network: bool,

    /// Container name (auto-generated if not set)
    pub name: Option<String>,

    /// Exec timeout (default: none)
    pub timeout: Option<Duration>,
}

pub struct Mount {
    /// Host path to mount
    pub source: PathBuf,

    /// Guest path to mount at
    pub target: String,

    /// Read-only mount
    pub read_only: bool,
}

pub struct PortMapping {
    /// Port on the host
    pub host: u16,

    /// Port inside the container
    pub container: u16,

    /// Protocol (default: tcp)
    pub protocol: Protocol,
}

pub enum Protocol {
    Tcp,
    Udp,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            cmd: vec![],
            env: vec![],
            working_dir: None,
            user: None,
            mounts: vec![],
            ports: vec![],
            cpus: None,
            memory_mb: None,
            network: true,
            name: None,
            timeout: None,
        }
    }
}
```

## Container

```rust
pub struct Container {
    id: String,
    name: String,
    /// Vm is Send but not Sync. Wrapped in Arc<Mutex> so Container can be
    /// shared across tasks (e.g., streaming output while calling exec).
    /// Alternatively, use an actor task that owns the Vm and accepts commands
    /// via a channel — see 01-safe-api.md line 301 for the pattern.
    vm: Arc<Mutex<Vm>>,
    /// Channel uses split reader/writer with internal Mutex, so concurrent
    /// send() and recv() are safe. See 05-base-prerequisites.md.
    agent: Channel<Request, Response>,
    config: RunConfig,
    image: ImageId,
    /// Mutex-wrapped because stop() takes &self but needs to mutate state.
    state: Mutex<ContainerStatus>,
}

pub enum ContainerStatus {
    Created,
    Running,
    Stopped { exit_code: i32 },
}

pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub image: String,
    pub status: ContainerStatus,
    pub created: SystemTime,
    pub cpus: u8,
    pub memory_mb: u64,
    pub ports: Vec<PortMapping>,
}

impl Container {
    /// Start a created container. Boots the VM and runs the entrypoint.
    pub async fn start(&mut self) -> Result<()>;

    /// Execute a command inside a running container.
    /// Accepts a single string which is shell-parsed into program + args,
    /// consistent with SandboxSession::exec() and Workspace::exec().
    pub async fn exec(
        &self,
        cmd: &str,
        timeout: Option<Duration>,
    ) -> Result<ExecOutput>;

    /// Execute a command and stream output.
    pub async fn exec_streaming(&self, cmd: &str) -> Result<ExecStream>;

    /// Stream the container's main process output (entrypoint stdout/stderr).
    pub fn output(&self) -> impl Stream<Item = Result<ExecEvent>>;

    /// Stop the container (SIGTERM → wait → SIGKILL).
    pub async fn stop(&self) -> Result<()>;

    /// Kill the container immediately (SIGKILL).
    pub async fn kill(&self) -> Result<()>;

    /// Get resource stats from inside the container.
    /// Returns a ResourceStats struct mapped from the protocol's ResourceStatsResult.
    pub async fn stats(&self) -> Result<ResourceStats>;

    /// Wait for the container's main process to exit.
    pub async fn wait(&self) -> Result<i32>;

    /// Get the container ID.
    pub fn id(&self) -> &str;

    /// Get the container name.
    pub fn name(&self) -> &str;

    /// Get the container status.
    pub fn status(&self) -> &ContainerStatus;
}
```

## ResourceStats

Application-level struct mapped from the protocol's `ResourceStatsResult` response:

```rust
pub struct ResourceStats {
    pub cpu_usage_percent: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub process_count: u32,
    pub load_average: [f64; 3],
}
```

This is returned by `Container::stats()` and `Workspace::stats()`. The type lives in `vz::protocol` (see `05-base-prerequisites.md`) so both `vz-sandbox` and `vz-oci` can use it.

## Container Lifecycle

### Create + Start (via `runtime.run()`)

```
runtime.run("python:3.12-slim", config)
  │
  ├── 1. Resolve image reference → "docker.io/library/python:3.12-slim"
  │
  ├── 2. Pull image if not cached
  │      ├── Download manifest, config, layers
  │      └── Unpack layers to ~/.vz/oci/layers/
  │
  ├── 3. Assemble rootfs
  │      ├── Stack layers bottom-to-top
  │      ├── Handle whiteouts
  │      └── Output: ~/.vz/oci/rootfs/<container-id>/
  │
  ├── 4. Build VmConfig
  │      ├── boot_linux(kernel, initramfs, cmdline)
  │      ├── shared_dir("rootfs", rootfs_path, read_only: true)
  │      ├── shared_dir("mount-0", config.mounts[0].source, ...)
  │      ├── enable_vsock()
  │      └── network_nat() if config.network else no network
  │
  ├── 5. Create + start VM
  │      └── Vm::create(vm_config).await? → vm.start().await?
  │
  ├── 6. Wait for guest agent
  │      ├── vm.vsock_connect(7424).await?
  │      ├── Send Handshake, receive HandshakeAck
  │      └── Timeout after 5s → error
  │
  ├── 7. Run entrypoint (if cmd specified)
  │      └── agent.send(Exec { command, args, env, working_dir, user })
  │
  └── 8. Return Container handle
```

### Exec

```rust
let output = container.exec("python -c \"print('hello')\"", None).await?;
assert_eq!(output.exit_code, 0);
assert_eq!(output.stdout.trim(), "hello");
```

Maps directly to the guest agent's `Exec` request. The container must be in `Running` state.

### Stop

```
container.stop()
  │
  ├── 1. Send Signal { SIGTERM } to entrypoint process
  ├── 2. Wait up to 10 seconds for exit
  ├── 3. If still running: send Signal { SIGKILL }
  ├── 4. Force-stop the VM: vm.stop().await
  └── 5. Update status to Stopped { exit_code }
```

### Remove

```
runtime.remove(container_id)
  │
  ├── 1. Ensure container is stopped (error if running)
  ├── 2. Delete assembled rootfs: ~/.vz/oci/rootfs/<container-id>/
  ├── 3. Remove from container registry
  └── 4. VM resources already released on stop
```

## Networking

### Default: NAT

Each container gets its own NAT network interface via Virtualization.framework. The container can reach the internet (for pip install, apt-get, etc.) but is not reachable from the host network.

### Port Forwarding

Port forwarding from host to container uses vsock tunneling:

```rust
RunConfig {
    ports: vec![
        PortMapping { host: 8080, container: 80, protocol: Protocol::Tcp },
    ],
    ..Default::default()
}
```

Implementation: the host listens on `0.0.0.0:8080` (TCP), and for each incoming connection, opens a vsock connection to the guest agent with a `PortForward` request. The agent connects to `localhost:80` inside the container and relays data bidirectionally.

This avoids needing to configure the VM's NAT port forwarding directly, which Virtualization.framework doesn't expose cleanly.

### Port Forward Protocol Extension

Add to the vsock protocol:

```rust
// Request (Host to Guest)
PortForward {
    id: u64,
    target_port: u16,
    protocol: String, // "tcp" or "udp"
}

// Response
PortForwardReady {
    id: u64,
}

// After PortForwardReady, the vsock stream becomes a raw bidirectional
// byte pipe to the target port. No more framing — just raw TCP relay.
```

This requires opening a second vsock connection for each forwarded port (the main connection on port 7424 continues to handle Exec/Signal/etc.).

### No Network Mode

```rust
RunConfig {
    network: false,
    ..Default::default()
}
```

When `network: false`, the VM is created without a network device. The container has no network interface at all — maximum isolation.

## Environment & Configuration

### Environment Variables

Merged from three sources (later overrides earlier):

1. **Image defaults** — From OCI image config `Env` field
2. **RunConfig.env** — User-specified overrides
3. **vz-injected** — `VZ_CONTAINER_ID`, `VZ_HOSTNAME`

```rust
fn build_env(image_config: &ImageConfig, run_config: &RunConfig, container_id: &str) -> Vec<(String, String)> {
    let mut env: HashMap<String, String> = HashMap::new();

    // Image defaults
    for var in &image_config.env {
        let (k, v) = var.split_once('=').unwrap_or((var, ""));
        env.insert(k.to_string(), v.to_string());
    }

    // User overrides
    for (k, v) in &run_config.env {
        env.insert(k.clone(), v.clone());
    }

    // vz-injected
    env.insert("VZ_CONTAINER_ID".to_string(), container_id.to_string());

    env.into_iter().collect()
}
```

### Working Directory

Resolution order:
1. `RunConfig.working_dir` (if set)
2. Image config `WorkingDir` (if set)
3. `/` (fallback)

### User

Resolution order:
1. `RunConfig.user` (if set)
2. Image config `User` (if set)
3. `root` (fallback — inside the container, root is safe because the VM is the isolation boundary)

## Container Metadata

Running and stopped container state is persisted at `~/.vz/oci/containers.json`:

```json
{
  "abc123": {
    "id": "abc123",
    "name": "happy-python",
    "image": "docker.io/library/python:3.12-slim",
    "image_id": "sha256:...",
    "status": "running",
    "created": "2026-02-17T10:30:00Z",
    "config": { "cmd": ["python", "app.py"], "cpus": 2, "memory_mb": 512 },
    "pid": 45678
  }
}
```

This allows `vz list` and `vz remove` to work across CLI invocations.

### Concurrency Safety

Multiple CLI invocations or library consumers can access `containers.json` concurrently. To prevent data corruption:

1. **Atomic writes** — Write to a temp file, then `rename()` (atomic on POSIX).
2. **Advisory file locking** — Use `flock()` on a `containers.json.lock` file before read-modify-write operations.

Given the expected scale (<100 containers), this is sufficient. No need for SQLite.

## Usage Examples

### Simple: Run a Command

```rust
let rt = Runtime::new(RuntimeConfig::default()).await?;

let container = rt.run("python:3.12-slim", RunConfig {
    cmd: vec!["python".into(), "-c".into(), "print('hello from vz')".into()],
    ..Default::default()
}).await?;

let exit = container.wait().await?;
// exit == 0
```

### Development: Mount Project, Install Deps, Run Tests

```rust
let container = rt.run("rust:1.85-slim", RunConfig {
    cmd: vec!["cargo".into(), "test".into()],
    mounts: vec![Mount {
        source: PathBuf::from("./my-project"),
        target: "/workspace".into(),
        read_only: false,
    }],
    working_dir: Some("/workspace".into()),
    ..Default::default()
}).await?;

let mut stream = container.output();
while let Some(event) = stream.next().await {
    match event? {
        ExecEvent::Stdout(data) => io::stdout().write_all(&data)?,
        ExecEvent::Stderr(data) => io::stderr().write_all(&data)?,
        ExecEvent::Exit(code) => println!("Tests exited: {code}"),
    }
}
```

### Long-Running Service

```rust
let container = rt.run("nginx:alpine", RunConfig {
    ports: vec![PortMapping { host: 8080, container: 80, protocol: Protocol::Tcp }],
    ..Default::default()
}).await?;

println!("nginx running at http://localhost:8080");
println!("Container ID: {}", container.id());

// Later...
container.stop().await?;
rt.remove(container.id()).await?;
```

### HQ Integration

```rust
// In HQ's worker backend
let rt = vz_oci::Runtime::new(config).await?;

// Agent session requests a Python environment
let container = rt.run("python:3.12", RunConfig {
    cmd: vec![],  // No entrypoint — just keep agent running
    mounts: vec![Mount {
        source: project_path.clone(),
        target: "/workspace".into(),
        read_only: false,
    }],
    network: false,  // No network — gate engine controls access
    ..Default::default()
}).await?;

// Execute agent commands inside the container
let result = container.exec(
    "python fix_bug.py",
    Some(Duration::from_secs(300)),
).await?;

// Release
container.stop().await?;
rt.remove(container.id()).await?;
```
