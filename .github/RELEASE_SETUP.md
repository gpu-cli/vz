# Release Setup

## Required GitHub Secrets

Configure these in **Settings > Secrets and variables > Actions** for `gpu-cli/vz`.

These are the same values used by `lightsofapollo/attn`:

| Secret | Description |
|--------|-------------|
| `APPLE_CERTIFICATE` | Base64-encoded `.p12` Developer ID certificate |
| `APPLE_CERTIFICATE_PASSWORD` | Password for the `.p12` file |
| `APPLE_SIGNING_IDENTITY` | `Developer ID Application: Conduit Ventures, Inc (QSL4ZJ5R3J)` |
| `APPLE_ID` | Apple Developer account email |
| `APPLE_APP_SPECIFIC_PASSWORD` | App-specific password for `notarytool` |
| `APPLE_TEAM_ID` | `QSL4ZJ5R3J` |
| `KEYCHAIN_PASSWORD` | Any random string (temporary CI keychain) |

### Copy from attn repo

If you have the values in `lightsofapollo/attn`, copy them:

```bash
# For each secret, read from attn and set on vz
# (GitHub CLI can't copy secrets directly — use the web UI or set manually)
for secret in APPLE_CERTIFICATE APPLE_CERTIFICATE_PASSWORD APPLE_SIGNING_IDENTITY \
              APPLE_ID APPLE_APP_SPECIFIC_PASSWORD APPLE_TEAM_ID KEYCHAIN_PASSWORD; do
  echo "Set $secret in gpu-cli/vz repo settings"
done
```

Or use the GitHub web UI: **github.com/gpu-cli/vz/settings/secrets/actions**

## Release Flow

1. Bump version in `crates/vz-cli/Cargo.toml`
2. Run `cd crates && cargo check` to update `Cargo.lock`
3. Commit and tag:
   ```bash
   git commit -m "Bump version to 0.2.0"
   git tag v0.2.0
   git push && git push origin v0.2.0
   ```
4. GitHub Actions builds, signs, notarizes, and publishes the release.

## What Gets Published

Each release includes:

| Artifact | Description |
|----------|-------------|
| `vz-v{VERSION}-darwin-arm64` | Signed + notarized CLI binary |
| `vz-guest-agent-v{VERSION}-darwin-arm64` | Guest agent binary (ad-hoc signed) |
| `vz-linux-v{VERSION}-arm64.tar.gz` | Linux kernel + initramfs + youki + `version.json` capability metadata |
| `vz-buildkit-v0.19.0-linux-arm64.tar` | Runtime-free BuildKit package containing only `buildkitd`, `buildctl`, and its checksum manifest |
| `*.sha256` | SHA256 checksums for all artifacts |

The Linux bundle release job validates `version.json` before publishing. The
metadata must include artifact checksums and the declared VZ guest capabilities
used by `vz-linux::ensure_kernel_bundle()`: `vsock`, `virtiofs`, `hvc0_serial`,
and `ext4_root`.

The BuildKit package is built from the pinned upstream source commit without
building or downloading an OCI runtime. Its deterministic archive inventory,
archive checksum, and per-file checksums must match the constants in
`vz-oci::buildkit`. The `v0.3.21` bootstrap is intentionally two phase: the
workflow and local release gate both call
`scripts/build-runtime-free-buildkit.sh`; the local gate does so automatically
and exactly once when a BuildKit suite is selected and both override variables
are absent. The gate then validates and retains an immutable run-owned copy,
its checksum, manifest, exact archive inventory, verification report, and build
provenance. `run-info.txt` and `summary.txt` record the source mode, builder
invocation count, digest, and evidence paths.

Setting `VZ_BUILDKIT_ARTIFACT_ARCHIVE` and
`VZ_BUILDKIT_ARTIFACT_SHA256` together selects an explicit operator override.
The harness copies it before validation and does not invoke the builder. This
mode is useful for debug-profile diagnosis, but it cannot supply or replace the
pinned builder's provenance. A release-profile BuildKit run rejects it before
guest/Cargo/VM work. Its run metadata starts with qualification pending and the
final summary records `buildkit_release_gate_qualified=true` only after a
validated candidate build, one builder invocation, complete provenance, and all
selected suites pass. Setting only one variable, setting either to blank, or
supplying an invalid path, checksum, or archive also fails before guest/Cargo/VM work begins.
There is no fallback among candidate, override, and published sources.

Once `v0.3.21` publishes the asset, add and run a separate, explicitly selected
published-source clean-install lane with no override and no candidate cache to
prove the default installer downloads the immutable release URL. Publication
must not silently change this harness from candidate-build mode. The
post-publication lane is distinct from the pre-release candidate-build lane;
neither may fall back to an upstream all-binaries archive, system Docker, or
another OCI runtime.

The source commit, Go patch toolchain downloads, build flags, and accepted output
digests are pinned. Host archive-tool changes can still alter package bytes, so
the workflow fails closed on any binary, header, inventory, or archive digest
change. Updating a pin requires a separately reviewed cross-host rebuild and
provenance check.

## User Installation

```bash
curl -sSf https://raw.githubusercontent.com/gpu-cli/vz/main/scripts/install.sh | sh
```

Or specific version:
```bash
VZ_VERSION=0.2.0 curl -sSf https://raw.githubusercontent.com/gpu-cli/vz/main/scripts/install.sh | sh
```

## Test Workflow (without publishing)

```bash
gh workflow run release.yml --ref main
```

This runs the full build + sign pipeline without creating a GitHub Release
(only tag pushes create releases).
