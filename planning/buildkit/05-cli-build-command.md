# 05 — CLI Build Command

## Depends On

- 04 (host build client — BuildClient with full protocol support)

## Problem

Users need a `vz build` command that feels like `docker build`. This is the CLI frontend that orchestrates the full pipeline: boot BuildKit VM if needed, submit build, show progress, handle output.

## Design

### Command Interface

```bash
# Basic build
vz build .

# Build with tag (pushes to vz local store)
vz build -t myapp:latest .

# Build with Dockerfile override
vz build -f Dockerfile.prod .

# Multi-stage target
vz build --target builder .

# Build args
vz build --build-arg VERSION=1.0 .

# Output to registry
vz build -t registry.example.com/myapp:v1 --push .

# Output as tarball
vz build -o type=oci,dest=./image.tar .

# No cache
vz build --no-cache .

# Secrets
vz build --secret id=npmrc,src=.npmrc .
```

### CLI Definition (clap)

```rust
#[derive(clap::Args)]
pub struct BuildArgs {
    /// Build context directory
    #[arg(default_value = ".")]
    context: PathBuf,

    /// Name and optionally tag (name:tag)
    #[arg(short = 't', long = "tag")]
    tag: Option<String>,

    /// Dockerfile path (relative to context)
    #[arg(short = 'f', long = "file", default_value = "Dockerfile")]
    dockerfile: PathBuf,

    /// Build target (multi-stage)
    #[arg(long)]
    target: Option<String>,

    /// Build arguments
    #[arg(long = "build-arg")]
    build_args: Vec<String>,

    /// Push to registry after build
    #[arg(long)]
    push: bool,

    /// Output specification
    #[arg(short = 'o', long = "output")]
    output: Option<String>,

    /// Disable cache
    #[arg(long)]
    no_cache: bool,

    /// Secrets
    #[arg(long = "secret")]
    secrets: Vec<String>,

    /// Progress output mode
    #[arg(long, default_value = "auto")]
    progress: ProgressMode,
}
```

### Progress Display

Three modes matching Docker/BuildKit convention:
- `auto` — fancy TTY output with live-updating steps (if terminal), plain if piped
- `plain` — sequential log output (good for CI)
- `tty` — force fancy output

Fancy TTY output:
```
[+] Building 12.3s (8/12)
 => [internal] load build definition from Dockerfile          0.0s
 => [internal] load .dockerignore                             0.0s
 => [1/6] FROM docker.io/library/rust:1.85-slim@sha256:...   0.0s (cached)
 => [2/6] WORKDIR /app                                       0.0s (cached)
 => [3/6] COPY Cargo.toml Cargo.lock ./                      0.1s
 => [4/6] RUN cargo fetch                                    8.2s
 => [5/6] COPY src/ src/                                     0.1s
 => [6/6] RUN cargo build --release                          ...
```

### Orchestration Flow

```
vz build -t myapp .
  ├── Validate context dir exists, Dockerfile present
  ├── Parse output mode (--push → Registry, -t → VzStore, -o → custom)
  ├── Ensure BuildKit VM is running (lazy boot)
  │   ├── Check if VM already warm → reuse
  │   └── If not → boot new VM (Phase 2)
  ├── Create gRPC channel (Phase 3)
  ├── Submit build (Phase 4)
  │   ├── FileSync: stream context from host
  │   ├── Auth: forward Docker creds
  │   └── Status: display progress
  ├── Handle output
  │   ├── VzStore: import into ~/.vz/oci/
  │   ├── Registry: push (already done by BuildKit)
  │   └── Tarball: write to dest
  ├── Print result summary
  └── Reset idle timeout (VM stays warm)
```

### Error Handling

- Missing Dockerfile → clear error with suggestion
- BuildKit VM boot failure → show buildkitd.log tail
- Build failure → show failing step's logs
- Registry auth failure → suggest `docker login`
- Network issues → check if guest has network access

### Implementation

New file: `crates/vz-cli/src/commands/build.rs`

Additions to: `crates/vz-cli/src/main.rs` (add `Build` subcommand)

Progress display: `crates/vz-cli/src/progress.rs` (new)

## Done When

1. `vz build .` builds a Dockerfile and imports result into vz store
2. `vz build -t myapp:latest .` tags the built image
3. `vz build --push -t registry/img:tag .` pushes to registry
4. Progress output works in TTY and plain modes
5. BuildKit VM auto-boots on first build, reuses on subsequent builds
6. Error messages are actionable
7. E2E test: `vz build` a simple Dockerfile, then `vz run` the result
