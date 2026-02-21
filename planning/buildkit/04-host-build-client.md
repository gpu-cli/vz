# 04 — Host Build Client

## Depends On

- 03 (vsock gRPC proxy — tonic Channel to buildkitd)

## Problem

We need a Rust client that speaks the BuildKit gRPC protocol: submitting builds, streaming context, forwarding auth, and receiving progress updates. This is the core integration layer.

## Design

### BuildKit Protocol Overview

BuildKit uses 3 concurrent gRPC streams for a build:

1. **`Control.Solve`** — submits the build request (Dockerfile frontend, output config)
2. **`Control.Status`** — streams real-time build progress (vertices, logs, timing)
3. **`Control.Session`** — bidirectional tunnel for daemon→client callbacks:
   - **FileSync**: daemon requests build context files on demand
   - **Auth**: daemon requests registry credentials on demand
   - **FileSend**: daemon sends output files (for local export)

The session tunnel is HTTP/2-over-gRPC: the client runs an HTTP/2 server inside the gRPC stream, and the daemon connects back as a client.

### Approach: Use `buildkit-client` Crate

The `buildkit-client` crate (v0.1.4) implements the full protocol in Rust:
- `SolveOptions` for build configuration
- FileSync provider for streaming build context
- Auth provider for Docker credential forwarding
- Progress streaming with vertex/log events

Evaluate this crate first. If it works with our tonic channel, use it directly. If not (e.g., assumes Unix socket, incompatible tonic version), fork or reimplement the critical pieces using `bollard-buildkit-proto` for generated types.

### Build Request

```rust
pub struct BuildRequest {
    pub context_dir: PathBuf,           // Local directory with source + Dockerfile
    pub dockerfile: Option<PathBuf>,    // Override Dockerfile path (default: ./Dockerfile)
    pub target: Option<String>,         // Multi-stage target
    pub build_args: HashMap<String, String>,  // --build-arg KEY=VALUE
    pub output: BuildOutput,            // Where to put the result
    pub no_cache: bool,                 // Disable cache
    pub secrets: Vec<SecretSpec>,       // --secret id=...,src=...
    pub ssh: Vec<SshSpec>,              // --ssh default=$SSH_AUTH_SOCK
    pub platform: Option<String>,       // Target platform (default: linux/arm64)
}

pub enum BuildOutput {
    /// Push to registry
    Registry { name: String },
    /// Export as OCI tarball
    OciTarball { dest: PathBuf },
    /// Export to local directory (rootfs)
    Local { dest: PathBuf },
    /// Import into vz's local image store for `vz run`
    VzStore { tag: String },
}
```

### Auth Provider

Reads `~/.docker/config.json` for registry credentials. The `docker_credential` crate (already a dependency in vz-oci) handles credential helpers (gcloud, ecr-login, etc.).

```rust
struct DockerAuthProvider;

impl AuthProvider for DockerAuthProvider {
    async fn credentials(&self, host: &str) -> Option<Credentials> {
        // Read ~/.docker/config.json
        // Resolve credential helper for host
        // Return username + password/token
    }
}
```

### FileSync Provider

Streams build context from the host filesystem. BuildKit's DiffCopy protocol requests files on demand (no need to tar the entire context upfront).

```rust
struct LocalFileSync {
    context_dir: PathBuf,
}

impl FileSyncProvider for LocalFileSync {
    // Respond to daemon's file read requests
    // Filter by .dockerignore patterns
}
```

### Progress Streaming

BuildKit `Status` stream emits `StatusResponse` with:
- **Vertexes**: build step nodes (cached/started/completed, name, error)
- **Logs**: stdout/stderr from each vertex
- **Warnings**: Dockerfile lint warnings

Map these to a progress UI (Phase 5 handles display).

```rust
pub enum BuildProgress {
    StepStarted { id: String, name: String },
    StepCached { id: String, name: String },
    StepCompleted { id: String, name: String, duration: Duration },
    StepFailed { id: String, name: String, error: String },
    Log { id: String, stream: LogStream, data: Vec<u8> },
    Warning { message: String, detail: Option<String> },
}
```

### VzStore Output

The `VzStore` output is unique to vz: export the built image directly into `~/.vz/oci/` so it's immediately available for `vz run`. This involves:

1. Export from BuildKit as OCI tarball (using `type=oci` exporter)
2. Unpack tarball into vz's image store (manifest, config, layers)
3. Register in vz's ref store (`~/.vz/oci/refs/`)

### Implementation

New modules in `vz-oci/src/buildkit/`:
- `client.rs` — `BuildClient` struct, wraps gRPC channel + session
- `auth.rs` — `DockerAuthProvider`
- `filesync.rs` — `LocalFileSync`
- `progress.rs` — progress event types and mapping
- `output.rs` — output handling (registry push, tarball, vz store import)

## Done When

1. `BuildClient::build(channel, request)` executes a Dockerfile build end-to-end
2. Build context streams from host to guest on demand (FileSync)
3. Registry auth forwards from host Docker config (Auth)
4. Progress events stream back for display
5. Output modes work: registry push, OCI tarball, local directory, vz store
6. `.dockerignore` is respected when streaming context
7. Integration test: build a simple Dockerfile, verify output image
