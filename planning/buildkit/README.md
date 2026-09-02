# BuildKit Integration — `docker build` for vz

## Vision

Enable `vz build .` to build OCI images from Dockerfiles, powered by BuildKit running inside a lightweight Linux guest VM. Users get full `docker build` compatibility (multi-stage, cache mounts, secrets, etc.) without Docker Desktop.

## Problem

vz can pull and run pre-built OCI images, but users can't build custom images. This is the #1 missing capability for a complete container workflow. Rather than reimplementing Dockerfile semantics, we run the real BuildKit daemon inside a guest VM and proxy its gRPC API over vsock.

## Architecture

```
Host (macOS)                              Guest (Linux VM)
┌─────────────────────┐                  ┌─────────────────────┐
│ vz build .          │                  │ buildkitd            │
│   │                 │                  │   ├── OCI worker     │
│   ├─ BuildClient    │──vsock:7425──────│   │   └─ runtime shim│
│   │  (Rust gRPC)    │                  │   └── /var/lib/      │
│   │                 │                  │       buildkit/ ext4 │
│   ├─ FileSync       │  (session)       │                      │
│   │  (context dir)  │◄─callback────────│  daemon calls back   │
│   ├─ Auth           │  (session)       │  for context + auth  │
│   │  (docker cfg)   │◄─callback────────│                      │
│   │                 │                  │                      │
│   └─ Progress UI    │◄─Status stream───│                      │
│                     │                  │                      │
│ BuildKit bin/       │──VirtioFS────────│ /mnt/buildkit-bin/   │
│ Linux bundle youki │──VirtioFS────────│ /mnt/linux-bin/youki │
│ BuildKit cache.img │──block device────│ /var/lib/buildkit/   │
└─────────────────────┘                  └─────────────────────┘
```

## Key Design Decisions

1. **Single vsock port for everything** — BuildKit's session protocol tunnels build context, auth credentials, and file exports through one gRPC connection. No separate VirtioFS mount needed for build context.

2. **Runtime-free BuildKit provisioning** — The checksum-pinned BuildKit package
   contains exactly `buildkitd`, `buildctl`, and its manifest. It contains no OCI
   runtime and is mounted read-only at `/mnt/buildkit-bin`.

3. **Pinned youki is the only OCI runtime** — The existing Linux bundle's
   checksum-pinned `youki` is mounted read-only at `/mnt/linux-bin/youki`.
   `buildkitd` targets `/tmp/vz-buildkit-oci-runtime`, an argv-preserving
   guest-agent multicall shim that execs only that youki binary and adds
   `--no-pivot` immediately after `create` and `run` while retaining every
   other argument as an opaque OS string. There is no runc or crun fallback.

4. **Rust gRPC client on host** — Use the `buildkit-client` crate (tonic/prost-based) for native Rust integration. Implements FileSync (build context streaming), Auth (Docker config forwarding), and Status (progress UI).

5. **Persistent ext4 cache disk** — Attach `~/.vz/buildkit/cache.img` as a
   sparse block device and mount it at `/var/lib/buildkit/`. It survives VM
   restarts and backs the overlay snapshotter.

6. **Dedicated BuildKit VM** — Separate from container runtime VMs. Long-lived daemon, boots on first `vz build`, stays warm for subsequent builds. Auto-shutdown after idle timeout.

7. **Output modes** — Push to registry, export as OCI tarball, or export directly to vz's local image store (`~/.vz/oci/`) for immediate `vz run`.

## Implementation Phases

| Phase | Doc | Description |
|-------|-----|-------------|
| 1 | [01-artifact-provisioning.md](01-artifact-provisioning.md) | Provision the runtime-free buildkitd + buildctl package |
| 2 | [02-guest-buildkit-service.md](02-guest-buildkit-service.md) | Boot buildkitd with pinned youki and the persistent cache disk |
| 3 | [03-vsock-grpc-proxy.md](03-vsock-grpc-proxy.md) | Bridge buildkitd socket over vsock to host |
| 4 | [04-host-build-client.md](04-host-build-client.md) | Rust BuildKit client with FileSync + Auth |
| 5 | [05-cli-build-command.md](05-cli-build-command.md) | `vz build` CLI command |
| 6 | [06-cache-management.md](06-cache-management.md) | Cache persistence, GC, prune |

## Constraints

- **arm64 only** — Apple Silicon guest VMs, BuildKit binaries must be arm64
- **One OCI runtime** — BuildKit executes only the mounted, pinned youki through
  the guest-agent shim; runc and crun are forbidden, with no fallback
- **Kernel contract** — the selected Linux bundle must declare `cgroup_bpf`, and
  the kernel must provide `CONFIG_BPF_SYSCALL=y` and `CONFIG_CGROUP_BPF=y`; the
  guest must mount cgroup v2 before buildkitd starts
- **Static mounts** — binary shares and the persistent cache disk are configured
  at VM boot
- **2 VM limit** — BuildKit VM counts toward macOS kernel's 2 concurrent VM limit
