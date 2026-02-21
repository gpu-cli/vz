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
│   │                 │                  │   ├── buildkit-runc  │
│   ├─ BuildClient    │──vsock:7425──────│   ├── overlayfs snap │
│   │  (Rust gRPC)    │                  │   └── /var/lib/      │
│   │                 │                  │       buildkit/      │
│   ├─ FileSync       │  (session)       │                      │
│   │  (context dir)  │◄─callback────────│  daemon calls back   │
│   ├─ Auth           │  (session)       │  for context + auth  │
│   │  (docker cfg)   │◄─callback────────│                      │
│   │                 │                  │                      │
│   └─ Progress UI    │◄─Status stream───│                      │
│                     │                  │                      │
│ ~/.vz/buildkit/     │──VirtioFS────────│  /var/lib/buildkit/  │
│   cache/            │  (persistent)    │  (layer cache)       │
└─────────────────────┘                  └─────────────────────┘
```

## Key Design Decisions

1. **Single vsock port for everything** — BuildKit's session protocol tunnels build context, auth credentials, and file exports through one gRPC connection. No separate VirtioFS mount needed for build context.

2. **Static binary provisioning** — buildkitd + buildkit-runc are ~84 MB static arm64 Linux binaries. Provisioned to `~/.vz/buildkit/bin/` and shared into guest via VirtioFS, same pattern as youki.

3. **Rust gRPC client on host** — Use the `buildkit-client` crate (tonic/prost-based) for native Rust integration. Implements FileSync (build context streaming), Auth (Docker config forwarding), and Status (progress UI).

4. **Persistent cache via VirtioFS** — Mount `~/.vz/buildkit/cache/` into guest at `/var/lib/buildkit/`. Survives VM restarts. Overlay snapshotter for layer dedup.

5. **Dedicated BuildKit VM** — Separate from container runtime VMs. Long-lived daemon, boots on first `vz build`, stays warm for subsequent builds. Auto-shutdown after idle timeout.

6. **Output modes** — Push to registry, export as OCI tarball, or export directly to vz's local image store (`~/.vz/oci/`) for immediate `vz run`.

## Implementation Phases

| Phase | Doc | Description |
|-------|-----|-------------|
| 1 | [01-artifact-provisioning.md](01-artifact-provisioning.md) | Download and manage buildkitd + runc static binaries |
| 2 | [02-guest-buildkit-service.md](02-guest-buildkit-service.md) | Boot Linux VM running buildkitd with cache mount |
| 3 | [03-vsock-grpc-proxy.md](03-vsock-grpc-proxy.md) | Bridge buildkitd socket over vsock to host |
| 4 | [04-host-build-client.md](04-host-build-client.md) | Rust BuildKit client with FileSync + Auth |
| 5 | [05-cli-build-command.md](05-cli-build-command.md) | `vz build` CLI command |
| 6 | [06-cache-management.md](06-cache-management.md) | Cache persistence, GC, prune |

## Constraints

- **arm64 only** — Apple Silicon guest VMs, BuildKit binaries must be arm64
- **No nested containers** — BuildKit uses runc directly (not Docker-in-Docker)
- **Static VirtioFS** — cache mount configured at VM boot, not runtime
- **2 VM limit** — BuildKit VM counts toward macOS kernel's 2 concurrent VM limit
