# 02 — Guest BuildKit Service

## Depends On

- 01 (artifact provisioning — buildkitd + runc binaries available)

## Problem

We need a Linux guest VM running `buildkitd` with proper configuration: overlay snapshotter, persistent cache via VirtioFS, and a listening socket for the host to connect to.

## Design

### VM Configuration

BuildKit gets a dedicated VM (not shared with container stacks). Configuration:

```rust
LinuxVmConfig {
    cpus: 4,                    // Builds are CPU-intensive
    memory_mb: 4096,            // 4 GB — BuildKit + runc + build processes
    shared_dirs: vec![
        // BuildKit binaries (read-only)
        SharedDirConfig {
            tag: "buildkit-bin",
            source: ~/.vz/buildkit/bin/,
            read_only: true,
        },
        // Persistent cache (read-write)
        SharedDirConfig {
            tag: "buildkit-cache",
            source: ~/.vz/buildkit/cache/,
            read_only: false,
        },
    ],
    serial_log_file: Some(~/.vz/buildkit/buildkitd.log),
    network_enabled: true,      // BuildKit needs network to pull base images
}
```

### Guest Init Modifications

The guest init script (or a post-boot exec sequence) needs to:

1. Mount VirtioFS shares:
   ```bash
   mount -t virtiofs buildkit-bin /mnt/buildkit-bin
   mount -t virtiofs buildkit-cache /var/lib/buildkit
   ```

2. Start buildkitd listening on TCP (for vsock proxy):
   ```bash
   /mnt/buildkit-bin/buildkitd \
     --addr tcp://0.0.0.0:8372 \
     --oci-worker-binary /mnt/buildkit-bin/buildkit-runc \
     --oci-worker-snapshotter overlayfs \
     --root /var/lib/buildkit
   ```

Port 8372 chosen to avoid conflicts. This is the port the vsock proxy connects to.

### Lifecycle

```
BuildkitVm::boot()
  ├── Ensure artifacts (Phase 1)
  ├── Configure LinuxVmConfig with buildkit shares + 4 CPU / 4 GB RAM
  ├── Boot Linux VM (existing vz-linux infrastructure)
  ├── Wait for guest agent ready (vsock port 7424)
  ├── Exec: mount VirtioFS shares
  ├── Exec: start buildkitd in background
  ├── Wait for buildkitd ready (health check TCP 8372)
  └── Return BuildkitVm handle

BuildkitVm::shutdown()
  ├── Send SIGTERM to buildkitd (graceful cache flush)
  ├── Wait for exit (timeout 10s)
  └── Stop VM
```

### Lazy Boot + Idle Shutdown

The BuildKit VM boots on first `vz build` and stays warm. After configurable idle timeout (default: 5 minutes with no builds), auto-shutdown to free resources. This mirrors Docker Desktop's behavior.

```rust
pub struct BuildkitVm {
    vm: Arc<LinuxVm>,
    last_build_at: Arc<Mutex<Instant>>,
    idle_timeout: Duration,
}
```

### Implementation

New module: `vz-oci/src/buildkit/vm.rs`

Key struct: `BuildkitVm` — wraps `LinuxVm` with buildkit-specific lifecycle.

## Done When

1. `BuildkitVm::boot()` starts a Linux VM with buildkitd running
2. buildkitd is reachable on guest TCP port 8372
3. VirtioFS cache mount at `/var/lib/buildkit/` persists across VM restarts
4. `BuildkitVm::shutdown()` gracefully stops buildkitd and VM
5. Idle timeout auto-shutdown works
6. Integration test: boot VM, verify buildkitd health check, shutdown
