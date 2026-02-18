# vz

Native macOS VM sandbox. Built on Apple's Virtualization.framework for Apple Silicon.

Create, snapshot, and restore macOS VMs in seconds. Execute commands inside VMs over vsock — no SSH, no network configuration, no manual setup.

```bash
# Create a golden macOS image (one-time, ~15 min)
vz init

# Boot a headless VM
vz run --name dev --headless &

# Execute commands inside the VM
vz exec dev -- uname -a
# Darwin Apple-Virtual-Machine 24.6.0 ... arm64

vz exec dev -- whoami
# dev

# Save state, restore later in seconds
vz save dev
vz run --name dev --restore ~/.vz/state/dev.vzsave --headless &
```

## Why vz

Existing macOS VM tools weren't built for automation. They require manual setup, SSH configuration, and can't snapshot/restore fast enough for CI or agent sandboxing.

vz is different:

- **No SSH.** Commands execute over vsock — a direct host-guest socket with zero network config.
- **Offline provisioning.** `vz init` produces a ready-to-use image with user account, auto-login, and guest agent. No Setup Assistant, no manual steps.
- **Save/restore.** Capture full VM state to disk. Restore to that exact state in seconds. Deterministic environments for CI and testing.
- **Native performance.** Runs directly on Apple's hypervisor. Hardware-accelerated, not emulated.
- **Built for automation.** Every operation is a CLI command. No GUI required.

## Install

Requires macOS 14+ (Sonoma) and Apple Silicon.

### From source

```bash
# Requires Rust 1.85+
git clone https://github.com/gpu-cli/vz.git
cd vz/crates
cargo build --workspace --release
./target/release/vz self-sign
```

The `self-sign` step adds the `com.apple.security.virtualization` entitlement required by macOS.

## Quick start

### 1. Create a golden image

```bash
# Downloads the latest macOS IPSW and installs it (~15 min)
vz init

# Or use a local IPSW
vz init --ipsw ~/Downloads/UniversalMac_15.3_24D60_Restore.ipsw
```

This creates `~/.vz/images/base.img` — a fully provisioned macOS disk image with:
- A `dev` user account with auto-login
- A guest agent that starts on boot and listens for commands

### 2. Provision the image

```bash
# Install the guest agent with proper ownership (requires sudo once)
sudo vz provision --image ~/.vz/images/base.img
```

### 3. Boot and run commands

```bash
# Start a headless VM
vz run --image ~/.vz/images/base.img --name my-vm --headless &

# Wait for boot (~30-45s on first cold boot)
sleep 45

# Run commands
vz exec my-vm -- echo "hello from the VM"
vz exec my-vm -- sw_vers
vz exec my-vm -- ls /Users/dev

# Stop
vz stop my-vm
```

### 4. Save and restore

```bash
# Save VM state to disk
vz save my-vm

# Later, restore in seconds instead of cold-booting
vz run --name my-vm --restore ~/.vz/state/my-vm.vzsave --headless &
```

## Architecture

```
vz-cli          CLI: init, run, exec, save, restore, stop, provision
    |
vz-sandbox      Pool management, sessions, typed channels
    |
vz              Safe async Rust API over Virtualization.framework
    |
objc2-virtualization    Auto-generated ObjC bindings (no hand-written FFI)
    |
Apple Virtualization.framework    macOS hypervisor (hardware-accelerated)
```

### How exec works

```
vz exec my-vm -- whoami
    |
    v
Unix socket (/~/.vz/run/my-vm.sock)
    |
    v
Control server (in vz run process)
    |
    v
vsock port 7424 (direct host-guest socket)
    |
    v
Guest agent (LaunchDaemon inside VM)
    |
    v
setuid(dev) -> spawn "whoami" -> stream stdout/stderr back
```

No TCP, no SSH keys, no port forwarding. vsock is a direct socket between host and guest kernel.

### Crates

| Crate | Purpose |
|-------|---------|
| **vz** | Safe async Rust API. VM lifecycle, vsock streams, VirtioFS, save/restore. 100% safe public API. |
| **vz-sandbox** | High-level sandbox. Pool of pre-warmed VMs, session management, wire protocol. |
| **vz-cli** | Standalone CLI. All operations available as commands. |
| **vz-guest-agent** | In-VM daemon. Listens on vsock, executes commands, streams output. |

## Linux artifacts (automatic)

For the Linux container backend (`vz-linux`), kernel/initramfs artifacts are built automatically via `linux/Makefile`.

```bash
# Local build (requires cross toolchain)
make -C linux all

# Reproducible build in Docker
make -C linux docker-build
```

Build outputs:

- `linux/out/vmlinux`
- `linux/out/initramfs.img`
- `linux/out/version.json`

CI also builds these automatically in `.github/workflows/linux-artifacts.yml` and uploads them as `vz-linux-artifacts`.

## CLI reference

| Command | Description |
|---------|-------------|
| `vz init` | Create a golden macOS image from an IPSW |
| `vz provision` | Provision an image with user account and guest agent |
| `vz run` | Start a VM (cold boot or restore from saved state) |
| `vz exec <name> -- <cmd>` | Execute a command inside a running VM |
| `vz save <name>` | Save VM state to disk |
| `vz restore <name>` | Restore VM from saved state |
| `vz stop <name>` | Stop a running VM |
| `vz list` | List running VMs |
| `vz self-sign` | Sign the binary with virtualization entitlement |

### Common options

```bash
# Custom resources
vz run --cpus 8 --memory 16 --name my-vm --headless

# Mount a host directory into the VM
vz run --mount project:/Users/dev/project --name my-vm --headless

# Run as a different user
vz exec my-vm --user root -- whoami

# Custom disk size
vz init --disk-size 128G
```

## Platform requirements

- **macOS 14+** (Sonoma) — required for save/restore
- **Apple Silicon** — macOS guests require Apple Silicon
- **2 concurrent VM limit** — kernel-enforced by Apple
- **Entitlement required** — binary must be signed with `com.apple.security.virtualization` (`vz self-sign` handles this)

## License

[FSL-1.1-MIT](LICENSE.md) — Free to use for any purpose except building a competing commercial product. Converts to MIT after 2 years.

## Contributing

Contributions welcome. Please open an issue before submitting large changes.

```bash
# Development
cd crates
cargo build --workspace
cargo clippy --workspace -- -D warnings
cargo nextest run --workspace
```
