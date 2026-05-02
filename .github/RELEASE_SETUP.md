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
| `*.sha256` | SHA256 checksums for all artifacts |

The Linux bundle release job validates `version.json` before publishing. The
metadata must include artifact checksums and the declared VZ guest capabilities
used by `vz-linux::ensure_kernel_bundle()`: `vsock`, `virtiofs`, `hvc0_serial`,
and `ext4_root`.

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
