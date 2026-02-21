# Phase 1: Install vdagent in Golden Image During Provisioning

Install UTM's SPICE guest tools (`spice-vdagent` + `spice-vdagentd`) into the mounted disk image so clipboard sharing works on first boot.

## Depends On

Phase 0 (host-side console device must be configured for the guest agent to connect to)

## Background

UTM maintains [vd_agent](https://github.com/utmapp/vd_agent) — a macOS port of the SPICE virtual display agent. It ships as a ~2 MB pkg containing:

- **`spice-vdagentd`** — LaunchDaemon (system-level, root). Communicates with the VirtIO console at `/dev/tty.com.redhat.spice.0`.
- **`spice-vdagent`** — LaunchAgent (per-user). Bridges the user's pasteboard to/from the daemon.

Pre-built releases: https://github.com/utmapp/vd_agent/releases

## Step 1: Download vdagent pkg during provisioning

**File**: `crates/vz-cli/src/provision.rs`

Add a function to download the UTM SPICE guest tools disk image:

```rust
const VDAGENT_VERSION: &str = "0.22.1";
const VDAGENT_URL: &str = "https://github.com/utmapp/vd_agent/releases/download/v0.22.1/utm-guest-tools-macos-0.22.1.img";
const VDAGENT_IMG_NAME: &str = "utm-guest-tools-macos-0.22.1.img";

fn download_or_cache_vdagent() -> anyhow::Result<PathBuf> {
    let cache_dir = dirs::home_dir().join(".vz/cache");
    let cached = cache_dir.join(VDAGENT_IMG_NAME);
    if cached.exists() {
        return Ok(cached);
    }
    // Download to cache (similar pattern to IPSW download)
    // Use reqwest blocking or shell out to curl
}
```

Cache the download at `~/.vz/cache/utm-guest-tools-macos-0.22.1.img` (same pattern as IPSW caching).

## Step 2: Extract binaries from the pkg

The `.img` file is a disk image containing a `.pkg` installer. To extract without running the installer:

```bash
# Mount the img
hdiutil attach utm-guest-tools-macos-0.22.1.img -mountpoint /tmp/vdagent-img

# The pkg is inside. Extract it:
pkgutil --expand /tmp/vdagent-img/spice-vdagent-0.22.1.pkg /tmp/vdagent-expanded

# Inside the expanded pkg, the Payload is a cpio archive:
cd /tmp/vdagent-expanded/spice-vdagent.pkg/
cat Payload | gunzip | cpio -idm

# This extracts:
# ./usr/local/bin/spice-vdagent
# ./usr/local/bin/spice-vdagentd
# ./Library/LaunchDaemons/org.spice-space.spice-vdagentd.plist
# ./Library/LaunchAgents/org.spice-space.spice-vdagent.plist
```

Implement this as `extract_vdagent_binaries(img_path: &Path) -> Result<VdagentFiles>` where `VdagentFiles` has paths to the extracted binaries and plists.

## Step 3: Install into mounted disk image

**File**: `crates/vz-cli/src/provision.rs`

Add `install_spice_vdagent(mount_point: &Path) -> anyhow::Result<()>`:

1. Download/cache the vdagent img
2. Extract binaries and plists from the pkg
3. Copy to the mounted disk image:
   - `<mount>/usr/local/bin/spice-vdagent` (mode 0755, owner root:wheel)
   - `<mount>/usr/local/bin/spice-vdagentd` (mode 0755, owner root:wheel)
   - `<mount>/Library/LaunchDaemons/org.spice-space.spice-vdagentd.plist` (mode 0644, owner root:wheel)
   - `<mount>/Library/LaunchAgents/org.spice-space.spice-vdagent.plist` (mode 0644, owner root:wheel)
4. Chown to root:wheel (same pattern as guest agent installation)

## Step 4: Wire into apply_auto_config

**File**: `crates/vz-cli/src/provision.rs`

In `apply_auto_config()`, after installing the guest agent:

```rust
// Install SPICE vdagent for clipboard sharing
if let Err(e) = install_spice_vdagent(mount_point) {
    warn!(error = %e, "failed to install SPICE vdagent (clipboard sharing will not work)");
}
```

Non-fatal — clipboard is nice-to-have, not required for core functionality.

## Step 5: Add --no-clipboard flag

**File**: `crates/vz-cli/src/commands/provision.rs`

Add `--no-clipboard` flag to `ProvisionArgs` to skip vdagent installation:

```rust
/// Skip SPICE vdagent installation (disables clipboard sharing).
#[arg(long)]
pub no_clipboard: bool,
```

## Validation

1. `cargo clippy --workspace -- -D warnings` — clean
2. `cargo nextest run --workspace` — all tests pass
3. Full E2E test:
   - `vz init` → `vz provision` (should download + install vdagent)
   - `vz run --image base.img --name test` (GUI mode)
   - Cmd+C some text on host → Cmd+V in VM terminal → should paste
4. Verify LaunchDaemon/Agent start on boot:
   - `vz exec test -- launchctl list | grep spice` shows both services
5. `~/.vz/cache/utm-guest-tools-macos-0.22.1.img` exists (cached)
6. Re-provision skips download (cache hit)

## Known Issues

- **macOS 15 clipboard bug**: UTM users report clipboard sharing stops working after a few minutes. `spice-vdagentd` becomes unresponsive. Workaround is restarting both processes. This is an upstream bug in vd_agent, not something we can fix. Monitor for updates.
- **Security prompts**: On first boot, macOS may prompt the user to allow `spice-vdagent` and `spice-vdagentd`. Since we're provisioning offline (not running the pkg installer interactively), we may need to pre-approve them via TCC database or MDM profile. Investigate if this is an issue.
- **GPL-3.0**: vd_agent is GPL-3.0. We ship it as a separate binary inside the VM (not linked into our code). This is the same model as any Linux distro shipping SPICE tools. Our code (FSL-1.1-MIT) is not affected.
