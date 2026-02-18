# 04 — Unified Runtime API (Linux + macOS)

## Purpose

The unified runtime provides a single API that abstracts over Linux containers (vz-oci + vz-linux) and macOS sandboxes (vz-sandbox). The caller writes one set of code; the runtime routes to the correct backend based on the image reference.

This is what HQ and other consumers use — they don't interact with vz-linux or vz-sandbox directly.

## Image Reference Routing

```
"python:3.12"              → Linux container (OCI pull from Docker Hub)
"ubuntu:24.04"             → Linux container (OCI pull from Docker Hub)
"ghcr.io/org/tool:v1"      → Linux container (OCI pull from GHCR)
"macos:sandbox"            → macOS sandbox (local golden image)
"macos:15"                 → macOS sandbox (local golden image)
```

Detection logic:

```rust
fn detect_backend(image: &str) -> Backend {
    // The "macos:" prefix is synthetic — it cannot collide with real OCI
    // registry references because OCI image names must contain a "/" before
    // the ":" (e.g., "registry/name:tag"), or be bare names like "ubuntu:24.04"
    // from Docker Hub's "library/" namespace. No real registry or namespace
    // is named "macos", so this prefix is safe for routing.
    if image.starts_with("macos:") {
        Backend::MacOS
    } else {
        Backend::Linux
    }
}
```

The `macos:` prefix is a synthetic reference — not a real registry image. Everything else is treated as an OCI image reference and pulled from a registry.

## Unified API

```rust
/// A workspace that can execute commands — either a Linux container or macOS sandbox.
pub struct Workspace {
    inner: WorkspaceInner,
}

enum WorkspaceInner {
    Linux(Container),
    MacOS {
        session: SandboxSession,
        /// Reference back to the pool for release on stop().
        pool: Arc<SandboxPool>,
    },
}

impl Workspace {
    /// Execute a command and wait for output.
    /// Both backends accept a single string and handle shell parsing internally.
    pub async fn exec(&self, cmd: &str, timeout: Option<Duration>) -> Result<ExecOutput> {
        match &self.inner {
            WorkspaceInner::Linux(c) => c.exec(cmd, timeout).await,
            WorkspaceInner::MacOS(s) => s.exec(cmd, timeout).await,
        }
    }

    /// Execute a command and stream output.
    pub async fn exec_streaming(&self, cmd: &str) -> Result<ExecStream> {
        match &self.inner {
            WorkspaceInner::Linux(c) => c.exec_streaming(cmd).await,
            WorkspaceInner::MacOS(s) => s.exec_streaming(cmd, None).await,
        }
    }

    /// Stop the workspace and release resources.
    pub async fn stop(self) -> Result<()> {
        match self.inner {
            WorkspaceInner::Linux(c) => c.stop().await,
            WorkspaceInner::MacOS { session, pool } => {
                // Release the session back to the pool so the VM can be reused.
                pool.release(session).await?;
                Ok(())
            }
        }
    }

    /// Get resource stats.
    pub async fn stats(&self) -> Result<ResourceStats> {
        match &self.inner {
            WorkspaceInner::Linux(c) => c.stats().await,
            WorkspaceInner::MacOS { session, .. } => {
                // Send ResourceStats request via guest agent
                // (same protocol, same agent, different OS)
                session.agent.request(Request::ResourceStats {
                    id: session.next_id(),
                }).await
                .and_then(|resp| match resp {
                    Response::ResourceStatsResult { cpu_usage_percent, memory_used_bytes, memory_total_bytes, disk_used_bytes, disk_total_bytes, process_count, load_average, .. } => {
                        Ok(ResourceStats { cpu_usage_percent, memory_used_bytes, memory_total_bytes, disk_used_bytes, disk_total_bytes, process_count, load_average })
                    }
                    _ => Err(anyhow::anyhow!("unexpected response")),
                })
            }
        }
    }

    /// Whether this workspace is a Linux container or macOS sandbox.
    pub fn backend(&self) -> &str {
        match &self.inner {
            WorkspaceInner::Linux(_) => "linux",
            WorkspaceInner::MacOS { .. } => "macos",
        }
    }
}
```

## WorkspaceManager

```rust
pub struct WorkspaceManager {
    linux_runtime: Runtime,
    macos_pool: Option<SandboxPool>,
    config: WorkspaceManagerConfig,
}

pub struct WorkspaceManagerConfig {
    /// Linux runtime config (kernel, initramfs, OCI store)
    pub linux: RuntimeConfig,

    /// macOS sandbox config (golden image, pool size)
    /// None = macOS sandboxes disabled
    pub macos: Option<SandboxConfig>,
}

impl WorkspaceManager {
    pub async fn new(config: WorkspaceManagerConfig) -> Result<Self>;

    /// Create a workspace from an image reference.
    /// Routes to Linux or macOS backend based on the reference.
    pub async fn create(&self, image: &str, config: WorkspaceConfig) -> Result<Workspace>;

    /// List all active workspaces.
    pub async fn list(&self) -> Vec<WorkspaceInfo>;
}
```

## WorkspaceConfig

```rust
/// Backend-agnostic workspace configuration.
pub struct WorkspaceConfig {
    /// Project directory to mount into the workspace.
    pub project_dir: PathBuf,

    /// Additional environment variables.
    pub env: Vec<(String, String)>,

    /// CPU cores (default depends on backend: 2 for Linux, 4 for macOS).
    pub cpus: Option<u8>,

    /// Memory in MB (default depends on backend: 512 for Linux, 8192 for macOS).
    pub memory_mb: Option<u64>,

    /// Enable network access (default: false for security).
    pub network: bool,

    /// Default exec timeout.
    pub timeout: Option<Duration>,
}
```

## CLI Integration

The `vz` CLI uses the unified API:

```
# Linux container (auto-detected from image reference)
vz run python:3.12-slim -- python -c "print('hello')"

# Linux container with mounts and ports
vz run -v ./project:/workspace -p 8080:80 node:22 -- npm start

# macOS sandbox
vz run macos:sandbox -- swift build

# List running workspaces
vz ps

  ID        IMAGE                BACKEND   STATUS    CPUS  MEM     UPTIME
  abc123    python:3.12-slim     linux     running   2     512 MB  5m
  def456    macos:sandbox        macos     running   4     8 GB    2h

# Exec into a running workspace
vz exec abc123 -- pip install requests

# Stop a workspace
vz stop abc123
```

### CLI Command Mapping

| Command | Description |
|---------|-------------|
| `vz run <image> [-- cmd]` | Create and start a workspace |
| `vz ps` | List running workspaces |
| `vz exec <id> -- <cmd>` | Execute a command in a running workspace |
| `vz stop <id>` | Stop a workspace |
| `vz rm <id>` | Remove a stopped workspace |
| `vz pull <image>` | Pre-pull an OCI image |
| `vz images` | List cached OCI images |
| `vz images prune` | Remove unused images and layers |
| `vz init` | Create macOS golden image (existing) |

## HQ Integration

```rust
// HQ worker backend
let wm = WorkspaceManager::new(config).await?;

// Gate engine determines which image to use based on the task
let image = match task.language {
    Language::Python => "python:3.12-slim",
    Language::Rust => "rust:1.85-slim",
    Language::Swift => "macos:sandbox",
    Language::Node => "node:22-alpine",
};

let workspace = wm.create(image, WorkspaceConfig {
    project_dir: task.repo_path.clone(),
    network: false,  // Gate engine controls network access
    timeout: Some(Duration::from_secs(600)),
    ..Default::default()
}).await?;

// Run agent commands
let result = workspace.exec("cargo test", None).await?;

// Get resource usage for cost tracking
let stats = workspace.stats().await?;
record_cost(stats.cpu_usage_percent, stats.memory_used_bytes);

// Release
workspace.stop().await?;
```

The gate engine doesn't need to know whether the workspace is Linux or macOS. It gates tool access via the same vsock protocol either way. Constraint inheritance, audit logging, and cost tracking are backend-agnostic.

## Backend Comparison

| Feature | Linux Container | macOS Sandbox |
|---------|----------------|---------------|
| Boot time | <2s (cold) | 5-10s (restore) |
| VM limit | Unlimited (RAM-bound) | 2 concurrent |
| Image source | OCI registries | Local golden image |
| Disk usage | Shared layers, no per-container disk | 64 GB disk image |
| Memory overhead | ~64-128 MB per VM | ~512 MB-1 GB per VM |
| Use case | Python, Rust, Node, Go, etc. | Swift, Xcode, macOS-native tools |
| Network default | Enabled (NAT) | Disabled |
| Session isolation | Ephemeral (VM destroyed on stop) | RestoreOnAcquire (clean snapshot) |

## Future: Docker Engine API Socket

For broad adoption beyond Rust consumers, expose a Docker Engine API on a Unix socket:

```
~/.vz/docker.sock
```

This would allow:
- `DOCKER_HOST=unix://~/.vz/docker.sock docker run python:3.12 python app.py`
- `docker-compose up` with a vz backend
- IDE Docker integrations (VS Code, IntelliJ) pointing at the vz socket

This is a significant amount of work (the Docker Engine API is large) but would make vz a drop-in Docker Desktop replacement for most workflows. Scope this as a separate project phase after the core runtime is proven.

### Minimum Viable Docker API

A subset that covers 90% of developer use cases:

| Endpoint | Docker CLI equivalent |
|----------|----------------------|
| `POST /containers/create` | `docker create` |
| `POST /containers/{id}/start` | `docker start` |
| `POST /containers/{id}/stop` | `docker stop` |
| `DELETE /containers/{id}` | `docker rm` |
| `GET /containers/json` | `docker ps` |
| `POST /containers/{id}/exec` | `docker exec` |
| `POST /images/create` | `docker pull` |
| `GET /images/json` | `docker images` |
| `POST /images/prune` | `docker image prune` |

Skip: volumes, networks, swarm, compose, build, logs streaming, events. These can be added incrementally.
