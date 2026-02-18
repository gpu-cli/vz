# 09 — Code Signing, Entitlements & Distribution

## Why Signing Matters

macOS binaries that use Virtualization.framework **must** have the `com.apple.security.virtualization` entitlement. Without it, the framework refuses to create VMs — `VZVirtualMachineConfiguration.validateWithError:` returns an error and the process cannot proceed. This is not optional.

Additionally, unsigned or ad-hoc signed binaries trigger Gatekeeper warnings on macOS, which is unacceptable for a developer tool. Users expect `brew install vz` or `curl | sh` to produce a binary that runs without security dialogs.

## Required Entitlements

### vz-cli (host binary)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.virtualization</key>
    <true/>
</dict>
</plist>
```

This single entitlement is required and sufficient for:
- Creating `VZVirtualMachine` instances
- Starting, stopping, pausing, resuming VMs
- Save/restore VM state
- VirtioFS shared directories
- Vsock communication

### Additional entitlements (conditional)

| Entitlement | When Needed | Notes |
|-------------|-------------|-------|
| `com.apple.security.virtualization` | Always | Core VM operations |
| `com.apple.security.network.client` | If using bridged networking | NAT networking does not require this |
| `com.apple.security.hypervisor` | Not needed | This is for Hypervisor.framework, not Virtualization.framework |

### vz-guest-agent (guest binary)

The guest agent binary runs **inside** the VM and does not use Virtualization.framework. It requires **no entitlements**. It is a plain Rust binary that uses vsock sockets (`AF_VSOCK`) and spawns child processes. Standard macOS binary, no special signing requirements beyond ad-hoc for Gatekeeper.

## Signing Identity

Signing uses the existing Conduit Ventures Developer ID certificate:

```
Developer ID Application: Conduit Ventures, Inc (QSL4ZJ5R3J)
```

This is the same identity used for company-agent releases. The certificate is stored as a base64-encoded `.p12` file in GitHub Actions secrets.

### Certificate Type: Developer ID Application

This is the correct certificate type for command-line tools distributed outside the Mac App Store. It enables:
- Code signing with `--options runtime` (hardened runtime)
- Notarization via `notarytool`
- Gatekeeper acceptance on end-user machines

Do **not** use "Apple Distribution" or "Mac App Store" certificates — those are for App Store distribution only.

## Signing Process

### Local Development

For local development and testing, ad-hoc signing is sufficient:

```bash
# Build the binary
cargo build --release -p vz-cli

# Ad-hoc sign with entitlements
codesign --sign - \
  --entitlements entitlements/vz-cli.entitlements.plist \
  --force \
  target/release/vz

# Verify
codesign --verify --verbose target/release/vz
codesign --display --entitlements - target/release/vz
```

Ad-hoc signing (`--sign -`) works for the developer's own machine but will trigger Gatekeeper warnings on other machines.

### Open-Source Contributors

Contributors building from source sign with ad-hoc:

```bash
cargo build --release -p vz-cli
codesign --sign - \
  --entitlements entitlements/vz-cli.entitlements.plist \
  --force \
  target/release/vz
```

This is documented in the project README under "Building from Source." The entitlements plist file is committed to the repository so contributors don't need to create it themselves.

### CI / Release Builds

Release builds use the Developer ID certificate with hardened runtime and notarization. The flow mirrors company-agent's existing release pipeline:

```bash
# 1. Import certificate into temporary keychain
CERTIFICATE_PATH="$RUNNER_TEMP/certificate.p12"
KEYCHAIN_PATH="$RUNNER_TEMP/app-signing.keychain-db"

echo -n "$APPLE_CERTIFICATE" | base64 --decode -o "$CERTIFICATE_PATH"
security create-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN_PATH"
security set-keychain-settings -lut 21600 "$KEYCHAIN_PATH"
security unlock-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN_PATH"
security import "$CERTIFICATE_PATH" -P "$APPLE_CERTIFICATE_PASSWORD" \
  -A -t cert -f pkcs12 -k "$KEYCHAIN_PATH"
security set-key-partition-list -S apple-tool:,apple: \
  -k "$KEYCHAIN_PASSWORD" "$KEYCHAIN_PATH"
security list-keychains -d user -s "$KEYCHAIN_PATH" login.keychain

# 2. Sign with Developer ID + hardened runtime
codesign --sign "Developer ID Application: Conduit Ventures, Inc (QSL4ZJ5R3J)" \
  --keychain "$KEYCHAIN_PATH" \
  --entitlements entitlements/vz-cli.entitlements.plist \
  --options runtime \
  --timestamp \
  --force \
  target/release/vz

# 3. Verify signature
codesign --verify --verbose target/release/vz

# 4. Clean up keychain
security delete-keychain "$KEYCHAIN_PATH"
```

Key flags:
- `--options runtime` — Hardened runtime, required for notarization
- `--timestamp` — Secure timestamp from Apple's TSA, required for notarization
- `--force` — Overwrite any existing signature (from cargo build)

## Notarization

Apple notarization verifies the binary is free of known malware and was signed with a valid Developer ID certificate. Notarized binaries pass Gatekeeper without warnings.

### Notarization Flow

```bash
# 1. Create a zip for submission (notarytool requires zip, dmg, or pkg)
ditto -c -k --keepParent target/release/vz vz-notarize.zip

# 2. Submit for notarization
xcrun notarytool submit vz-notarize.zip \
  --apple-id "$APPLE_ID" \
  --password "$APPLE_APP_SPECIFIC_PASSWORD" \
  --team-id "$APPLE_TEAM_ID" \
  --wait

# 3. Staple the notarization ticket (for dmg/pkg only — not applicable to bare binaries)
# Bare command-line tools cannot be stapled. Gatekeeper checks the notarization
# ticket online when the user first runs the binary. This is standard for CLI tools.

# 4. Clean up
rm vz-notarize.zip
```

**Stapling note**: Bare Mach-O binaries cannot have notarization tickets stapled to them. This is an Apple limitation. The ticket is stored on Apple's servers and verified by Gatekeeper on first launch. This works fine for CLI tools — the user needs internet on first run, which is reasonable since they just downloaded the binary.

### Notarization Timing

Notarization typically takes 1-5 minutes. The `--wait` flag blocks until complete. For CI, this is acceptable.

## Repository File Layout

```
vz/
├── entitlements/
│   └── vz-cli.entitlements.plist    # Committed to repo
├── .github/
│   └── workflows/
│       ├── ci.yml                   # Build + test (all platforms)
│       ├── release.yml              # Tagged release (sign + notarize + publish)
│       └── test-signing.yml         # Verify signing works (PR check)
└── ...
```

## CI Workflows

### ci.yml (Build + Test)

Runs on every push/PR:
- **Linux**: Unit tests only (Layer 1) — no macOS needed
- **macOS ARM64**: Unit + integration tests (Layers 1-2), ad-hoc signing verification

### release.yml (Tagged Release)

Triggered by tag push (`v*`):

1. **Build** — `cargo build --release -p vz-cli` on macOS ARM64 runner
2. **Sign** — Developer ID certificate from secrets
3. **Notarize** — Submit to Apple, wait for approval
4. **Package** — Create tar.gz with the signed binary + SHA256 checksum
5. **Publish** — Upload to GitHub Releases
6. **Homebrew** — Update Homebrew tap formula (when ready)

### test-signing.yml (Signing Verification)

Runs on demand or as part of release prep:
- Import certificate, sign a test binary, verify signature
- Catches certificate expiration or secret misconfiguration before release day

## GitHub Actions Secrets

Reuse existing secrets from company-agent where possible:

| Secret | Value | Shared with company-agent? |
|--------|-------|---------------------------|
| `APPLE_CERTIFICATE` | Base64-encoded .p12 Developer ID cert | Yes — same cert |
| `APPLE_CERTIFICATE_PASSWORD` | Password for .p12 file | Yes |
| `APPLE_ID` | Apple Developer account email | Yes |
| `APPLE_APP_SPECIFIC_PASSWORD` | App-specific password for notarytool | Yes |
| `APPLE_TEAM_ID` | `QSL4ZJ5R3J` | Yes |
| `KEYCHAIN_PASSWORD` | Random per-workflow (generated in CI) | N/A — ephemeral |

Since vz is a separate repository, these secrets need to be configured in vz's GitHub repo settings. They are the same values as company-agent — same Developer ID, same certificate, same Apple account.

## Distribution Channels

### 1. GitHub Releases (Primary)

Every tagged release publishes:
- `vz-darwin-arm64.tar.gz` — Signed + notarized macOS binary
- `vz-darwin-arm64.tar.gz.sha256` — Checksum
- `vz-guest-agent-darwin-arm64.tar.gz` — Guest agent binary (ad-hoc signed)

### 2. Homebrew Tap

```ruby
class Vz < Formula
  desc "macOS VM sandbox for coding agents"
  homepage "https://github.com/conduit-ventures/vz"
  url "https://github.com/conduit-ventures/vz/releases/download/v#{version}/vz-darwin-arm64.tar.gz"
  sha256 "..."

  depends_on :macos
  depends_on arch: :arm64

  def install
    bin.install "vz"
  end

  test do
    system "#{bin}/vz", "--version"
  end
end
```

Hosted in a `homebrew-tap` repository. Updated automatically by the release workflow.

### 3. cargo install (Build from Source)

```bash
cargo install vz-cli
```

Users who install via cargo must ad-hoc sign the binary themselves:

```bash
codesign --sign - \
  --entitlements <(curl -sL https://raw.githubusercontent.com/conduit-ventures/vz/main/entitlements/vz-cli.entitlements.plist) \
  --force \
  $(which vz)
```

This is documented clearly in the README. Consider providing a `vz self-sign` command that does this automatically post-install.

### 4. Install Script

```bash
curl -sSf https://vz.dev/install | sh
```

The install script:
1. Detects architecture (arm64 only — fail with clear message on Intel)
2. Downloads the latest signed binary from GitHub Releases
3. Verifies SHA256 checksum
4. Installs to `~/.vz/bin/vz`
5. Adds to PATH (or prints instructions)

## Certificate Lifecycle

The Developer ID Application certificate expires after 5 years. Current certificate details:

- **Identity**: Developer ID Application: Conduit Ventures, Inc (QSL4ZJ5R3J)
- **Team ID**: QSL4ZJ5R3J

When the certificate approaches expiration:
1. Generate a new certificate in Apple Developer portal
2. Export as .p12
3. Base64 encode and update `APPLE_CERTIFICATE` secret in GitHub
4. Update `APPLE_CERTIFICATE_PASSWORD` if changed
5. The signing identity name stays the same

## Hardened Runtime Implications

The `--options runtime` flag enables macOS Hardened Runtime, which restricts:
- JIT compilation (not relevant for Rust binaries)
- DYLD environment variables (not relevant)
- Debugging by other processes (not relevant for CLI tools)
- Unsigned executable memory (not relevant for Rust)

Rust binaries are naturally compatible with hardened runtime — no additional entitlements are needed to allow JIT or unsigned memory. The only entitlement we need is `com.apple.security.virtualization`.
