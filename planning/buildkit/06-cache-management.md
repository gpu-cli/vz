# 06 — Cache Management

## Depends On

- 02 (guest BuildKit service — VirtioFS cache mount)
- 05 (CLI — need a place to expose cache commands)

## Problem

BuildKit's layer cache is the key to fast builds. We need persistent cache that survives VM restarts, configurable size limits, and user-facing commands for cache inspection and cleanup.

## Design

### Cache Storage

```
~/.vz/buildkit/cache/          # VirtioFS-mounted into guest at /var/lib/buildkit/
├── runc-overlayfs/
│   ├── content/               # Content-addressable blobs
│   └── snapshots/             # Overlay snapshots (layer diffs)
└── cache.db                   # Metadata database (bbolt)
```

BuildKit manages this directory internally. We just need to:
1. Ensure the VirtioFS mount exists (Phase 2)
2. Configure GC policy in buildkitd config
3. Expose cache operations through CLI

### BuildKit GC Configuration

Written to guest filesystem before starting buildkitd:

```toml
# /etc/buildkit/buildkitd.toml (inside guest)
[worker.oci]
  gc = true
  snapshotter = "overlayfs"

[[worker.oci.gcpolicy]]
  keepDuration = "168h"    # 7 days
  all = true

[[worker.oci.gcpolicy]]
  keepBytes = 10737418240  # 10 GB max cache
  all = true
```

### CLI Commands

```bash
# Show cache usage
vz build cache du
# Output:
#   ID            RECLAIMABLE   SIZE          LAST ACCESSED
#   abc123...     true          245.3 MB      2 hours ago
#   Total:        1.82 GB

# Prune all unused cache
vz build cache prune

# Prune cache older than 24h
vz build cache prune --keep-duration 24h

# Prune to specific size
vz build cache prune --keep-storage 5GB

# Remove entire cache
vz build cache prune --all
```

These map to BuildKit's `DiskUsage` and `Prune` gRPC RPCs.

### Implementation

CLI additions: `crates/vz-cli/src/commands/build.rs` (add `cache` subcommand group)

Cache operations: `vz-oci/src/buildkit/cache.rs`

```rust
pub async fn disk_usage(channel: Channel) -> Result<Vec<CacheEntry>> {
    // Call Control.DiskUsage RPC
}

pub async fn prune(channel: Channel, opts: PruneOptions) -> Result<PruneSummary> {
    // Call Control.Prune RPC (streaming response)
}
```

## Done When

1. `vz build cache du` shows cache usage from buildkitd
2. `vz build cache prune` frees unused cache entries
3. Cache persists across `vz build` invocations (VM restarts)
4. GC policy limits cache growth automatically
5. `vz build cache prune --all` removes everything
6. Cache directory can be deleted manually (`rm -rf ~/.vz/buildkit/cache/`) as escape hatch
