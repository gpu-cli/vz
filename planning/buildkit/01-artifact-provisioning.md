# 01 — Artifact Provisioning

## Depends On

Nothing — foundation phase.

## Problem

BuildKit requires `buildkitd` (daemon) and `buildkit-runc` (OCI runtime for build steps) to run inside the guest VM. These are static arm64 Linux binaries available from GitHub releases (~84 MB tarball). We need a provisioning system similar to how we manage kernel artifacts and youki.

## Design

### Storage Layout

```
~/.vz/buildkit/
├── bin/
│   ├── buildkitd          # ~50 MB static arm64 binary
│   └── buildkit-runc      # ~10 MB static arm64 binary
├── cache/                 # Persistent layer cache (Phase 6)
└── version.json           # {"buildkit": "0.19.0", "downloaded_at": "..."}
```

### Download Source

GitHub releases: `https://github.com/moby/buildkit/releases/download/v{VERSION}/buildkit-v{VERSION}.linux-arm64.tar.gz`

Tarball contains `bin/buildkitd`, `bin/buildctl`, `bin/buildkit-runc`, `bin/buildkit-qemu-*`. We only need `buildkitd` and `buildkit-runc`.

### Version Pinning

Pin BuildKit version in code (like kernel version). Start with latest stable (v0.19.x series). Version stored in `version.json` for upgrade detection.

### Provisioning Flow

```
ensure_buildkit_artifacts()
  ├── Check ~/.vz/buildkit/version.json exists and matches pinned version
  ├── If missing or outdated:
  │   ├── Download tarball from GitHub releases
  │   ├── Verify SHA256 checksum
  │   ├── Extract buildkitd + buildkit-runc to bin/
  │   ├── chmod +x both binaries
  │   └── Write version.json
  └── Return BuildkitArtifacts { buildkitd_path, runc_path }
```

### Implementation

New module in `vz-oci`: `src/buildkit/artifacts.rs`

```rust
pub struct BuildkitArtifacts {
    pub bin_dir: PathBuf,        // ~/.vz/buildkit/bin/
    pub cache_dir: PathBuf,      // ~/.vz/buildkit/cache/
    pub version: String,         // "0.19.0"
}

pub async fn ensure_buildkit_artifacts() -> Result<BuildkitArtifacts, BuildkitError> {
    // Check version, download if needed, return paths
}
```

Should reuse the download + extraction patterns from `vz-linux/src/kernel.rs` (the `ensure_kernel_with_options` function).

## Done When

1. `ensure_buildkit_artifacts()` downloads and caches BuildKit binaries on first call
2. Subsequent calls are no-ops when version matches
3. `buildkitd` and `buildkit-runc` are valid arm64 ELF binaries
4. Version upgrade replaces old binaries
5. Unit test verifies version check logic (integration test downloads real binary)
