# 01 — Artifact Provisioning

## Depends On

Nothing — foundation phase.

## Problem

BuildKit requires the static Linux arm64 `buildkitd` daemon and `buildctl`
client in the guest VM. The upstream all-binaries release archive also contains
an OCI runtime, so vz must not download or repackage it. Release automation
instead builds only these two commands from the pinned BuildKit source. The
guest uses the youki binary from the existing Linux artifact bundle as its sole
OCI runtime.

## Design

### Storage Layout

```
~/.vz/buildkit/
├── bin/
│   ├── buildkitd          # ~50 MB static arm64 binary
│   └── buildctl           # static arm64 client
├── cache/                 # Persistent layer cache (Phase 6)
└── version.json           # version, layout, source, archive, and binary hashes
```

### Download Source

The vz release workflow builds the `buildctl` and `buildkitd` Dockerfile targets
from the immutable BuildKit v0.19.0 source commit. It publishes a deterministic
ustar package named `vz-buildkit-v0.19.0-linux-arm64.tar` on the vz release.

The `v0.3.21` rollout is intentionally two phase. The release workflow first
produces the candidate archive, and pre-release VM/E2E runs consume that exact
file through the local override below. After the `v0.3.21` release publishes the
asset, the default installer can fetch it from the immutable release URL. Until
then, a fresh install without the override fails closed; it never falls back to
an upstream all-binaries archive or another OCI runtime.

The archive inventory is exact:

```
manifest.json
bin/buildctl
bin/buildkitd
```

The manifest records the BuildKit version, source commit, platform, layout
generation, and independently pinned SHA-256 for each binary. The installer
pins the whole-archive SHA-256 as well and rejects every extra entry.

### Version Pinning

Pin the BuildKit version, immutable source commit, artifact layout, archive
SHA-256, and both binary SHA-256 values in code. Installed `version.json`
retains that complete contract for cache validation. Layout generation 2 is the
first runtime-free layout; older metadata and any directory with extra files
are replaced atomically while the persistent build cache remains intact.

### Provisioning Flow

```
ensure_buildkit_artifacts()
  ├── Check version.json, layout, source, platform, archive hash, binary hashes
  ├── Require bin/ inventory to be exactly buildctl + buildkitd
  ├── If missing or outdated:
  │   ├── Read the checksum-pinned vz release archive
  │   ├── Verify archive, manifest, and per-binary SHA-256 values
  │   ├── Reject every non-allowlisted archive entry
  │   ├── Stage buildkitd + buildctl privately
  │   └── Atomically replace bin/ and version.json
  └── Return BuildkitArtifacts { bin_dir, cache_dir, version }
```

For a release candidate or local VM lane, set both
`VZ_BUILDKIT_ARTIFACT_ARCHIVE=/absolute/path/to/archive.tar` and
`VZ_BUILDKIT_ARTIFACT_SHA256=<sha256>`. The local archive must satisfy the same
pinned manifest and per-file digests as the published artifact.

The source commit and every accepted output digest are pinned, but rebuilding is
not guaranteed to be bit-for-bit reproducible if upstream builder base images or
the release runner's archive tooling drift. Such drift fails closed because the
workflow checks the expected binary and archive digests. Updating those pins
requires a separately reviewed rebuild and provenance check.

### Implementation

New module in `vz-oci`: `src/buildkit/artifacts.rs`

```rust
pub struct BuildkitArtifacts {
    pub bin_dir: PathBuf,        // ~/.vz/buildkit/bin/
    pub cache_dir: PathBuf,      // ~/.vz/buildkit/cache/
    pub version: String,         // "0.19.0"
}

pub fn ensure_buildkit_artifacts() -> Result<BuildkitArtifacts, BuildkitError> {
    // Check version, download if needed, return paths
}
```

Should reuse the download + extraction patterns from `vz-linux/src/kernel.rs` (the `ensure_kernel_with_options` function).

## Done When

1. No BuildKit artifact consumed or published by vz contains an OCI runtime
2. `buildkitd` and `buildctl` are independently checksum-pinned
3. Archive and installed inventories are exact allowlists
4. Legacy layouts and caches with extra binaries are replaced
5. Local pre-release packages use the same manifest and checksum contract
6. Unit tests cover archive, manifest, binary, inventory, and migration checks
