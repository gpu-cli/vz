# 07 — IPSW to Bootable macOS VM

## What's a Golden Image

A pre-configured macOS disk image with dev tools installed, ready to clone or snapshot.

## IPSW Sources

- **Apple CDN**: `VZMacOSRestoreImage.latestSupported` gives the URL
- **Local file**: User-provided `.ipsw`
- macOS version compatibility: must match or be compatible with host macOS version

## Image Creation Pipeline

```
IPSW -> VZMacOSInstaller -> Raw macOS -> First Boot Setup -> Dev Tools -> Golden Image
```

### Step 1: Download IPSW

```rust
VZMacOSRestoreImage::fetchLatestSupportedWithCompletionHandler
```

Downloads ~13GB IPSW. Show progress via callback.

### Step 2: Create Disk Image

- Create sparse disk image (APFS container)
- Minimum 32GB, recommended 64GB for dev tools
- `VZDiskImageStorageDeviceAttachment` with read/write

### Step 3: Platform Configuration

For macOS guests, need:

- `VZMacPlatformConfiguration` with:
  - `hardwareModel` (from restore image)
  - `machineIdentifier` (generate unique)
  - `auxiliaryStorage` (NVRAM, ~512KB)
- These must be saved alongside the disk image for future boots

### Step 4: Install macOS

`VZMacOSInstaller` runs the install:

```rust
let installer = VZMacOSInstaller(virtualMachine: vm, restoreImageURL: ipsw_url)
installer.install(completionHandler: { error in ... })
// Progress via installer.progress (NSProgress)
```

Takes 10-30 minutes depending on hardware.

### Step 5: First Boot Configuration

After install, boot the VM with a display for Setup Assistant:

- Skip Apple ID (Cmd+Q or skip button)
- Create local admin account
- Agree to terms
- Skip diagnostics/analytics

This step currently requires user interaction. Future: automate via AppleScript or a configuration profile.

### Step 6: Install Dev Tools

SSH or guest agent into the VM:

```bash
# Xcode CLI tools (essential for any build)
xcode-select --install
# Or accept license and install non-interactively
sudo xcodebuild -license accept

# Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Common tools
brew install git cmake pkg-config
```

### Step 7: Clean & Snapshot

- Clear caches, logs, temp files
- Shut down cleanly
- The disk image is now the golden image
- Save VM state for fast restore: `vm.save_state(path)`

## Image Storage

```
~/.vz/
├── images/
│   ├── base.img          # Golden disk image
│   └── base.aux          # Auxiliary storage (NVRAM)
├── states/
│   └── base.state        # Saved VM state
└── platform/
    ├── hardware-model.bin
    └── machine-id.bin
```

## Image Versioning

- Each golden image is versioned by macOS version + toolchain
- APFS cloning for copy-on-write snapshots (if needed)
- For the long-lived VM model, typically just one golden image that gets updated in-place

## Constraints

- IPSW must be compatible with host macOS version
- Platform identity (hardware model, machine ID) must match between boots
- Auxiliary storage must persist alongside disk image
- First boot requires user interaction (no fully automated provisioning yet)
