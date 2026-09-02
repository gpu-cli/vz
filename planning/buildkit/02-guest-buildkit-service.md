# 02 — Guest BuildKit Service

## Depends On

- 01 (runtime-free artifact provisioning — `buildkitd` and `buildctl` available)
- Linux bundle provisioning (checksum-pinned `youki` and `cgroup_bpf` capability)

## Problem

We need a Linux guest VM running `buildkitd` with the overlay snapshotter, a
persistent ext4 cache disk, and a listening socket for the host. The BuildKit
package must remain runtime-free: its OCI worker can execute only the pinned
`youki` from the existing Linux bundle.

## Design

### VM Configuration

BuildKit gets a dedicated VM (not shared with container stacks). Configuration:

```rust
LinuxVmConfig {
    cpus: 4,                    // Builds are CPU-intensive
    memory_mb: 8192,            // 8 GB — BuildKit and build processes
    disk_image: Some(~/.vz/buildkit/cache.img), // 64 GiB sparse ext4 disk
    shared_dirs: vec![
        // Runtime-free BuildKit binaries (read-only; exact buildkitd/buildctl)
        SharedDirConfig {
            tag: "buildkit-bin",
            source: ~/.vz/buildkit/bin/,
            read_only: true,
        },
        // Existing Linux bundle directory containing pinned youki (read-only)
        SharedDirConfig {
            tag: "linux-bin",
            source: <resolved Linux bundle directory>,
            read_only: true,
        },
    ],
    serial_log_file: Some(~/.vz/buildkit/buildkitd.log),
    network_enabled: true,      // BuildKit needs network to pull base images
}
```

### Guest Init Modifications

The guest setup sequence must fail closed while it:

1. Mount VirtioFS shares:
   ```bash
   mount -t virtiofs buildkit-bin /mnt/buildkit-bin
   mount -t virtiofs linux-bin /mnt/linux-bin
   mount -t ext4 /dev/vda /var/lib/buildkit
   ```
   `/mnt/buildkit-bin` must contain executable `buildkitd` and `buildctl`, but no
   OCI runtime. `/mnt/linux-bin/youki` must be executable.

2. Mount cgroup v2 at `/sys/fs/cgroup`. Before the VM is created, the host also
   requires the selected bundle metadata to declare `cgroup_bpf`; a bundle that
   omits it is rejected rather than attempted. BuildKit's OCI workloads require
   this kernel support for cgroup device filtering, backed by
   `CONFIG_BPF_SYSCALL=y` and `CONFIG_CGROUP_BPF=y`.

3. Install the guest-agent multicall shim:
   ```bash
   ln -sf /usr/bin/vz-guest-agent /tmp/vz-buildkit-oci-runtime
   ```
   When invoked through this name, the agent preserves BuildKit's argv as raw OS
   strings, ensures `--no-pivot` is immediately after the `create` or `run`
   subcommand, and `exec`s `/mnt/linux-bin/youki`. Every other argument retains
   its order and byte representation. The shim does not use a shell, search
   `PATH`, translate the command to another runtime, or fall back to runc/crun.

4. Configure and start buildkitd on loopback (for the vsock proxy):
   ```bash
   cat >/etc/buildkit/buildkitd.toml <<'EOF'
   [worker.oci]
     binary = "/tmp/vz-buildkit-oci-runtime"
     gc = true
     snapshotter = "overlayfs"
   EOF

   /mnt/buildkit-bin/buildkitd \
     --config /etc/buildkit/buildkitd.toml \
     --addr tcp://127.0.0.1:8372 \
     --oci-worker-binary /tmp/vz-buildkit-oci-runtime \
     --oci-worker-snapshotter overlayfs \
     --root /var/lib/buildkit
   ```

Port 8372 chosen to avoid conflicts. This is the port the vsock proxy connects to.

### Lifecycle

```
BuildkitVm::boot()
  ├── Ensure artifacts (Phase 1)
  ├── Resolve the Linux bundle and require its cgroup_bpf declaration
  ├── Configure the BuildKit, youki, and cache mounts + 4 CPU / 8 GB RAM
  ├── Boot Linux VM (existing vz-linux infrastructure)
  ├── Wait for guest agent ready (vsock port 7424)
  ├── Exec: mount binary shares, cache disk, and cgroup v2
  ├── Exec: install the guest-agent runtime shim and write buildkitd.toml
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
3. The ext4 cache disk at `/var/lib/buildkit/` persists across VM restarts
4. `BuildkitVm::shutdown()` gracefully stops buildkitd and VM
5. Idle timeout auto-shutdown works
6. Runtime inventory proves the worker/shim resolves only to
   `/mnt/linux-bin/youki`, no forbidden runtime binary is present, and retained
   buildkitd argv names the shim
7. Integration test: boot VM, complete a build, verify `create`/`run` execution
   evidence and the youki-only inventory, then shutdown
