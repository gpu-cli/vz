# 07 — IPSW to Bootable macOS VM

## What's a Golden Image

A pre-configured macOS disk image with dev tools installed, ready to clone or snapshot.

## IPSW Resolution (Local-First)

`VZMacOSInstaller` requires an IPSW (or equivalent restore image) to install macOS into a VM. The OS on the user's machine cannot be cloned directly — Apple's Sealed System Volume prevents byte-level copying, and the VM needs its own platform identity and bootloader state.

However, the IPSW is often **already on the user's machine**. We check local sources before downloading anything.

### Resolution Order

```
1. --ipsw <path>              User-provided file (explicit, skip all detection)
2. /Applications/Install macOS*.app   macOS installer app (from App Store or softwareupdate)
3. ~/.vz/cache/*.ipsw         Previously downloaded by vz
4. Apple CDN (last resort)    Download ~13 GB from Apple
```

### Source 1: User-Provided IPSW

```bash
vz init --ipsw ~/Downloads/UniversalMac_15.2_24C101_Restore.ipsw
```

No detection, no download. Used when the user has already downloaded the IPSW manually or has it from a previous VM tool (Tart, UTM, etc.).

### Source 2: macOS Installer App

The macOS installer app (`/Applications/Install macOS Sequoia.app`) contains a valid restore image at:

```
/Applications/Install macOS Sequoia.app/Contents/SharedSupport/SharedSupport.dmg
```

This file is a valid input to `VZMacOSRestoreImage.loadFromFileAt:`. Many developers have this app — anyone who has upgraded macOS, downloaded it from the App Store, or run `softwareupdate --fetch-full-installer`.

Detection:

```rust
fn find_local_installer() -> Option<PathBuf> {
    let apps_dir = PathBuf::from("/Applications");
    for entry in std::fs::read_dir(&apps_dir).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("Install macOS") && name.ends_with(".app") {
            let shared_support = entry.path()
                .join("Contents/SharedSupport/SharedSupport.dmg");
            if shared_support.exists() {
                return Some(shared_support);
            }
        }
    }
    None
}
```

If found, the user sees:

```
Found macOS installer at /Applications/Install macOS Sequoia.app
Using local installer — no download needed.
```

### Source 3: vz Cache

If vz has previously downloaded an IPSW, it's cached at `~/.vz/cache/`:

```
~/.vz/cache/
└── UniversalMac_15.2_24C101_Restore.ipsw    # ~13 GB, kept until user deletes
```

Checked automatically. If the cached version is compatible with the host, it's used without prompting.

### Source 4: Apple CDN (Last Resort)

If no local source is found, `vz init` must download from Apple. This is the worst case and needs excellent UX to not feel like a broken experience.

#### Pre-Download Disclosure

Before downloading, clearly explain what's happening and why:

```
No local macOS installer found.

vz needs a macOS restore image (IPSW) to create the sandbox VM.
This is a one-time download — future VMs restore from a saved snapshot in seconds.

  Download size:  ~13.4 GB
  Disk space needed:
    IPSW (cached):    ~13.4 GB
    VM disk image:    ~64.0 GB (sparse — actual usage grows with installed software)
    Saved state:      ~4.0 GB
    Total:            ~81.4 GB (maximum, typically ~40 GB initially)

  Source: Apple CDN (official macOS restore image)
  Cache location: ~/.vz/cache/

You can skip this download by providing a local IPSW:
  vz init --ipsw /path/to/restore.ipsw

Or download the macOS installer app first (also ~13 GB, but reusable):
  softwareupdate --fetch-full-installer

Proceed with download? [Y/n]
```

#### Download UX

The download must feel reliable and professional:

```
Downloading macOS 15.2 restore image...

  ████████████████████░░░░░░░░░░  67%  8.9 GB / 13.4 GB  •  42 MB/s  •  1m 48s remaining

  Destination: ~/.vz/cache/UniversalMac_15.2_24C101_Restore.ipsw
```

Requirements:
- **Resumable** — If interrupted (Ctrl+C, network drop, laptop sleep), resume from where it left off on next `vz init`. Use HTTP Range headers. Store partial download as `*.ipsw.partial` with a metadata sidecar recording bytes received.
- **Progress with ETA** — Download speed, bytes transferred, estimated time remaining. Updated every second.
- **Integrity verification** — Validate the downloaded IPSW with `VZMacOSRestoreImage.loadFromFileAt:` before proceeding. If corrupt, delete and re-download.
- **Ctrl+C is safe** — Partial download is preserved. User can resume later. Print: `"Download paused. Run 'vz init' again to resume."`

#### Resumable Download Implementation

```rust
struct DownloadState {
    url: String,
    total_bytes: u64,
    downloaded_bytes: u64,
    etag: Option<String>,
}

// Stored at ~/.vz/cache/download-state.json alongside the .ipsw.partial file
// On resume:
// 1. Read state file
// 2. Send HTTP GET with Range: bytes={downloaded_bytes}-
// 3. Append to .partial file
// 4. On completion, rename .partial -> .ipsw, delete state file
```

#### Post-Download Tip

After the download completes:

```
Download complete. Cached at ~/.vz/cache/UniversalMac_15.2_24C101_Restore.ipsw

Tip: This file is only needed to create new VMs. You can free 13.4 GB by running:
  vz cache clean
```

### Space Management UX

#### vz init — Pre-Flight Space Check

Before starting any work, check available disk space:

```rust
fn check_disk_space(needs_download: bool) -> Result<()> {
    let available = fs2::available_space("~/.vz")?;

    let required = if needs_download {
        // IPSW + disk image + state + overhead
        13_400_000_000 + 64_000_000_000 + 4_000_000_000 + 2_000_000_000
    } else {
        // Disk image + state + overhead (no IPSW download)
        64_000_000_000 + 4_000_000_000 + 2_000_000_000
    };

    if available < required {
        // Print clear error with actionable advice
    }
    Ok(())
}
```

If space is insufficient:

```
Insufficient disk space.

  Available:  38.2 GB
  Required:   ~81.4 GB (13.4 GB download + 64 GB VM disk + 4 GB saved state)

Options:
  1. Free up disk space and try again
  2. Use a smaller VM disk: vz init --disk-size 32G (minimum for dev tools)
  3. Use an external drive: vz init --output /Volumes/External/.vz/
```

#### vz cache — Manage Cached Files

```bash
vz cache list          # Show cached files and sizes
vz cache clean         # Delete cached IPSWs (golden images and states are kept)
vz cache clean --all   # Delete everything in ~/.vz/cache/
```

Example output:

```
vz cache list

  ~/.vz/cache/
    UniversalMac_15.2_24C101_Restore.ipsw    13.4 GB    (downloaded 2026-02-15)

  ~/.vz/images/
    base.img                                  22.1 GB    (sparse, 64 GB max)
    base.img.aux                               0.5 MB
    base.img.hwmodel                           0.1 KB
    base.img.machineid                         0.1 KB

  ~/.vz/states/
    base.state                                 3.8 GB

  Total: 39.3 GB
```

#### Sparse Disk Images

The 64 GB disk image is created **sparse** — it only consumes disk space as data is written to it. A fresh macOS installation uses ~15-20 GB. The 64 GB is the maximum, not the initial footprint.

This should be communicated clearly:

```
Creating VM disk image...
  Size: 64 GB (sparse — starts at ~15 GB, grows as you install software)
```

### Version Compatibility

The IPSW must be compatible with the host's macOS version and Apple Silicon hardware. `VZMacOSRestoreImage` provides compatibility checking:

```rust
// After loading the restore image, check compatibility
let restore_image = VZMacOSRestoreImage::load_from(path).await?;
let supported = restore_image.mostFeaturefulSupportedConfiguration();
if supported.is_none() {
    // This IPSW is not compatible with this Mac
    // (e.g., too new for the hardware, or wrong architecture)
}
```

If a local installer is found but incompatible:

```
Found macOS installer at /Applications/Install macOS Ventura.app
  Version: macOS 13.6 — incompatible (vz requires macOS 14+ for save/restore)

Downloading compatible restore image from Apple...
```

## Image Creation Pipeline

```
IPSW Resolution -> VZMacOSInstaller -> Auto-Config -> First Boot -> Dev Tools -> Golden Image
```

### Step 1: Create Disk Image

- Create sparse disk image (APFS container)
- Minimum 32GB, recommended 64GB for dev tools
- `VZDiskImageStorageDeviceAttachment` with read/write

### Step 2: Platform Configuration

For macOS guests, need:

- `VZMacPlatformConfiguration` with:
  - `hardwareModel` (from restore image)
  - `machineIdentifier` (generate unique)
  - `auxiliaryStorage` (NVRAM, ~512KB)
- These must be saved alongside the disk image for future boots

### Step 3: Install macOS

`VZMacOSInstaller` runs the install using the resolved IPSW:

```rust
let installer = VZMacOSInstaller(virtualMachine: vm, restoreImageURL: ipsw_url)
installer.install(completionHandler: { error in ... })
// Progress via installer.progress (NSProgress)
```

Takes 10-30 minutes depending on hardware.

```
Installing macOS 15.2 into VM...

  ████████████████████████░░░░░░  80%  •  ~4 minutes remaining

  This is a one-time setup. Future sessions restore in seconds.
```

### Step 4: Auto-Configuration (Unattended)

Applied directly to the disk image before first boot. See "Automated First-Boot Provisioning" section below.

### Step 5: First Boot (Unattended)

Boot the VM headless. macOS completes initial setup without user interaction (Setup Assistant is skipped, dev user is pre-created, auto-login is enabled). Guest agent starts via launchd.

### Step 6: Provision Dev Tools (via Guest Agent)

The host connects to the guest agent over vsock and runs the provisioning script. See "Dev Tool Provisioning Script" section below.

### Step 7: Save State & Finish

```
Finalizing sandbox image...

  Cleaning caches and temp files
  Saving VM state for instant restore

Done! Sandbox ready.

  Image:  ~/.vz/images/base.img  (18.4 GB used, 64 GB max)
  State:  ~/.vz/states/base.state  (3.8 GB)
  Cache:  ~/.vz/cache/UniversalMac_15.2_24C101_Restore.ipsw  (13.4 GB, safe to delete)

  Total disk usage: 35.6 GB

  Run 'vz run' to start the sandbox.
  Run 'vz cache clean' to free 13.4 GB (IPSW no longer needed).
```

## Image Storage

```
~/.vz/
├── images/
│   ├── base.img              # Golden disk image
│   ├── base.img.aux          # Auxiliary storage (NVRAM)
│   ├── base.img.hwmodel      # Hardware model (from restore image)
│   └── base.img.machineid    # Machine identifier (generated)
├── states/
│   └── base.state            # Saved VM state for fast restore
└── cache/
    └── *.ipsw                # Cached IPSW downloads
```

Platform identity files (`.aux`, `.hwmodel`, `.machineid`) live alongside the disk image they belong to, following the convention established in the `vz` crate (see `01-safe-api.md`). This ensures all files needed to boot a VM are co-located and can be moved together.

## Image Versioning

- Each golden image is versioned by macOS version + toolchain
- APFS cloning for copy-on-write snapshots (if needed)
- For the long-lived VM model, typically just one golden image that gets updated in-place

## Automated First-Boot Provisioning

### The Problem

The current pipeline requires manual interaction with Setup Assistant (skip Apple ID, create user account, agree to terms). This breaks the promise of `vz init` being a zero-friction experience.

### Strategy: Auto-Configuration Profile

macOS supports configuration profiles (`.mobileconfig`) that can be applied during first boot to skip Setup Assistant screens and pre-configure user accounts. This is the same mechanism used by MDM enrollment, but we use it locally without an MDM server.

#### Skip Setup Assistant

Create a configuration profile that marks Setup Assistant as complete:

```bash
# Create the marker file on the guest disk image
# This tells macOS to skip Setup Assistant entirely
touch /Volumes/GuestDisk/private/var/db/.AppleSetupDone
```

#### Pre-Create User Account

Create the `dev` user account directly on the disk image before first boot:

```bash
# Create user record
dscl -f /Volumes/GuestDisk/private/var/db/dslocal/nodes/Default \
  localonly -create /Local/Default/Users/dev
dscl -f /Volumes/GuestDisk/private/var/db/dslocal/nodes/Default \
  localonly -create /Local/Default/Users/dev UserShell /bin/zsh
dscl -f /Volumes/GuestDisk/private/var/db/dslocal/nodes/Default \
  localonly -create /Local/Default/Users/dev UniqueID 501
dscl -f /Volumes/GuestDisk/private/var/db/dslocal/nodes/Default \
  localonly -create /Local/Default/Users/dev PrimaryGroupID 20
dscl -f /Volumes/GuestDisk/private/var/db/dslocal/nodes/Default \
  localonly -create /Local/Default/Users/dev NFSHomeDirectory /Users/dev

# Create home directory
mkdir -p /Volumes/GuestDisk/Users/dev
```

This approach is used by Tart and other CI-focused macOS VM tools. It works because the user database is stored on disk in a known location.

#### Auto-Login

Enable auto-login for the `dev` user so the VM reaches a usable state without any login interaction:

```bash
# Set auto-login user
defaults write /Volumes/GuestDisk/Library/Preferences/com.apple.loginwindow \
  autoLoginUser -string "dev"
```

### Dev Tool Provisioning Script

After first boot (which now completes unattended), the `vz init` command connects via vsock and runs a provisioning script:

```bash
#!/bin/bash
set -euo pipefail

# Accept Xcode license
sudo xcodebuild -license accept 2>/dev/null || true

# Install Xcode Command Line Tools (non-interactive)
sudo xcode-select --install 2>/dev/null || true
# Wait for installation to complete
until xcode-select -p &>/dev/null; do sleep 5; done

# Install Homebrew (non-interactive)
NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
eval "$(/opt/homebrew/bin/brew shellenv)"

# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# Common dev tools
brew install git cmake pkg-config

# Install and start the guest agent
sudo cp /mnt/tools/vz-guest-agent /usr/local/bin/vz-guest-agent
sudo cp /mnt/tools/com.vz.guest-agent.plist /Library/LaunchDaemons/
sudo launchctl load /Library/LaunchDaemons/com.vz.guest-agent.plist
```

The provisioning script and guest agent binary are provided via a read-only VirtioFS mount (`tools` tag) that is configured during `vz init`.

### Pre-Built Golden Images (Future)

For users who don't want to wait for IPSW download + macOS installation + provisioning (which takes 30-60 minutes), we can distribute pre-built golden images:

1. Build golden images in CI on Apple Silicon runners
2. Upload to an OCI registry or CDN
3. `vz init --prebuilt` downloads the image instead of building from scratch

**Constraints:**
- macOS EULA may restrict redistribution of macOS disk images. Needs legal review.
- Platform identity (hardware model, machine ID) must be regenerated per-machine — cannot ship a single identity.
- Image size is large (15-30 GB compressed).

**Alternative:** Ship a "recipe" (the provisioning script + config profile) instead of the image itself. `vz init` builds the image locally but uses Apple's IPSW directly. This avoids EULA concerns while still automating the process.

### vz init Full Flow (Automated)

```
vz init
  │
  ├── Pre-flight checks
  │   ├── Apple Silicon? (fail with clear message on Intel)
  │   ├── macOS 14+? (fail with upgrade instructions)
  │   ├── Disk space sufficient? (fail with actionable advice)
  │   └── Entitlements? (check binary is signed, offer vz self-sign)
  │
  ├── Resolve IPSW (local-first)
  │   ├── Check /Applications/Install macOS*.app → "Using local installer — no download needed"
  │   ├── Check ~/.vz/cache/*.ipsw → "Using cached restore image"
  │   └── Download from Apple CDN → disclosure, confirmation, resumable download
  │
  ├── Create disk image (64 GB sparse)
  │
  ├── Generate platform identity (hardware model + machine ID)
  │
  ├── Run VZMacOSInstaller (~10-30 min)
  │   └── Progress bar with ETA
  │
  ├── Apply auto-configuration (to disk image, before boot)
  │   ├── Touch .AppleSetupDone
  │   ├── Create dev user (UID 501)
  │   ├── Enable auto-login
  │   └── Install guest agent + launchd plist
  │
  ├── First boot (unattended, ~2 min)
  │   ├── macOS completes initial setup
  │   └── Guest agent starts automatically
  │
  ├── Provision dev tools (via guest agent, ~5-10 min)
  │   ├── Xcode CLI tools
  │   ├── Homebrew
  │   ├── Rust (optional, prompted)
  │   └── Common tools (git, cmake)
  │
  ├── Save VM state (warm snapshot with agent running, ~30s)
  │
  └── Summary
      ├── Disk usage breakdown (image, state, cache)
      ├── Tip: "vz cache clean" to free IPSW
      └── Next step: "vz run"
```

### Timing Breakdown

| Step | With local installer | With download |
|------|---------------------|---------------|
| Pre-flight + IPSW resolution | ~5s | ~5s |
| IPSW download | — | 5-20 min (network dependent) |
| macOS installation | 10-30 min | 10-30 min |
| First boot + provisioning | 5-10 min | 5-10 min |
| Save state | ~30s | ~30s |
| **Total** | **~15-40 min** | **~20-60 min** |

If the macOS installer app is already on disk, the user saves 5-20 minutes and 13.4 GB of bandwidth. This is the common case for active macOS developers.

After `vz init` completes, all subsequent `vz run` invocations restore from saved state in **5-10 seconds**. The init cost is paid once.

## Constraints

- IPSW must be compatible with host macOS version
- Platform identity (hardware model, machine ID) must match between boots
- Auxiliary storage must persist alongside disk image
