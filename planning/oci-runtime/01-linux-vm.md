# 10 — vz-linux: Lightweight Linux VM Bootstrap

## Purpose

`vz-linux` provides everything needed to boot a minimal Linux VM on Apple's Virtualization.framework in under 2 seconds. It ships a pre-compiled arm64 Linux kernel, a minimal initramfs containing the guest agent, and handles the boot sequence from kernel load to "guest agent reachable on vsock."

This is the foundation for running OCI containers as hardware-isolated VMs on macOS.

## Why Not a Full Distro

Running Ubuntu or Alpine as a VM guest means booting systemd/OpenRC, initializing dozens of services, configuring networking — 10-30 seconds before the VM is usable. For a container runtime where each container is its own VM, this latency is unacceptable.

Instead, we ship a minimal Linux kernel + a custom initramfs:

| Component | Full Distro VM | vz-linux |
|-----------|---------------|----------|
| Kernel | 30+ MB, thousands of modules | ~10 MB, stripped to essentials |
| Init system | systemd (200+ units) | Custom init script (~50 lines) |
| Userspace | Full distro (500 MB+) | busybox + guest agent (~15 MB) |
| Boot time | 10-30s | <2s |
| Disk image | Required (2+ GB) | None — rootfs via VirtioFS |

## Architecture

```
┌───────────────────────────────────────────────────────┐
│ macOS Host                                            │
│                                                       │
│  vz-linux::LinuxVm                                    │
│    │                                                  │
│    ├── kernel: ~/.vz/linux/vmlinux (arm64, ~10 MB)    │
│    ├── initramfs: ~/.vz/linux/initramfs.img (~15 MB)  │
│    │     contains: /init, /bin/busybox,               │
│    │               /usr/bin/vz-guest-agent             │
│    │                                                  │
│    └── VirtioFS mount: container rootfs               │
│         host: ~/.vz/oci/rootfs/<container-id>/        │
│         guest: /mnt/rootfs                            │
│                                                       │
│  vsock (port 7424) ◄──────────────────────────────┐   │
│                                                   │   │
│  ┌───────────────── Linux VM ──────────────────┐  │   │
│  │                                             │  │   │
│  │  kernel boots → /init runs →                │  │   │
│  │  mount VirtioFS at /mnt/rootfs →            │  │   │
│  │  switch_root into /mnt/merged →              │  │   │
│  │  start vz-guest-agent on vsock ─────────────┼──┘   │
│  │  exec container entrypoint                  │      │
│  │                                             │      │
│  └─────────────────────────────────────────────┘      │
└───────────────────────────────────────────────────────┘
```

## Linux Kernel

### Configuration

A custom kernel config targeting the minimum needed for a Virtualization.framework guest:

**Include:**
- Virtio drivers (virtio-blk, virtio-net, virtio-fs, virtio-vsock, virtio-console)
- VirtioFS (FUSE + virtiofs)
- Overlayfs (for OCI layer stacking)
- Networking (TCP/IP, virtio-net driver)
- Namespaces and cgroups (for optional inner container isolation)
- tmpfs, devtmpfs, proc, sysfs
- ext4 (for scratch volumes)
- AF_VSOCK socket family

**Exclude:**
- All physical hardware drivers (USB, PCI, SCSI, SATA, NVMe, GPU, audio, etc.)
- Bluetooth, WiFi, infrared
- All filesystems except ext4, overlayfs, tmpfs, proc, sysfs, virtiofs
- Kernel debugging, profiling, ftrace
- Security modules (SELinux, AppArmor) — the VM itself is the security boundary
- Module loading — build everything needed as built-in (no initrd module loading)

### Build

```bash
# Cross-compile arm64 Linux kernel on macOS (or build on Linux ARM64)
make ARCH=arm64 CROSS_COMPILE=aarch64-linux-gnu- defconfig
# Apply vz-linux .config overlay
scripts/kconfig/merge_config.sh .config vz-linux.config
make ARCH=arm64 CROSS_COMPILE=aarch64-linux-gnu- -j$(nproc) Image
```

The output is `arch/arm64/boot/Image` (~10 MB uncompressed). This is what `VZLinuxBootLoader` expects.

### Versioning

The kernel version is pinned in `vz-linux/Cargo.toml` metadata:

```toml
[package.metadata.linux]
kernel_version = "6.12"
kernel_sha256 = "..."
```

Kernel updates are explicit and tested. Users don't need to build the kernel themselves — it's distributed as a binary artifact alongside the `vz-cli` release.

### Distribution

The kernel binary is bundled with `vz-cli` releases:

```
vz-cli release tarball:
├── vz                           # CLI binary (signed + notarized)
├── linux/
│   ├── vmlinux                  # arm64 Linux kernel
│   └── initramfs.img            # initramfs with guest agent
└── entitlements/
    └── vz-cli.entitlements.plist
```

On first use, copied to `~/.vz/linux/`. The `vz` library crate (`vz-linux`) embeds the path and verifies the files exist at runtime.

## Initramfs

### Contents

```
initramfs/
├── init                         # PID 1 — shell script
├── bin/
│   └── busybox                  # Minimal userspace (mount, sh, ip, etc.)
│       (symlinks: mount, umount, sh, ip, hostname, cat, ls, mkdir, chroot, switch_root)
├── usr/bin/
│   └── vz-guest-agent           # Same protocol as macOS, compiled for Linux
├── etc/
│   ├── resolv.conf              # DNS: nameserver 8.8.8.8 (for containers needing network)
│   └── udhcpc.script            # Minimal DHCP callback for busybox udhcpc
├── dev/                         # Minimal device nodes (created by init)
├── proc/                        # Mount point
├── sys/                         # Mount point
└── mnt/
    ├── rootfs/                  # VirtioFS mount point for container rootfs
    ├── overlay-work/            # tmpfs for overlayfs upper/work dirs
    └── merged/                  # overlayfs merged mount point
```

### /init Script

```bash
#!/bin/busybox sh

# Helper: print to console and halt on fatal error
fatal() {
    echo "FATAL: $1" > /dev/console 2>&1
    echo "VM init failed. The host will see a timeout." > /dev/console 2>&1
    sleep infinity
}

# Mount essential filesystems
/bin/busybox mount -t proc proc /proc || fatal "failed to mount /proc"
/bin/busybox mount -t sysfs sysfs /sys || fatal "failed to mount /sys"
/bin/busybox mount -t devtmpfs devtmpfs /dev || fatal "failed to mount /dev"
/bin/busybox mount -t tmpfs tmpfs /tmp

# Mount the container rootfs from VirtioFS
# The host shares the OCI rootfs under the "rootfs" tag
/bin/busybox mount -t virtiofs rootfs /mnt/rootfs || fatal "failed to mount rootfs via VirtioFS — is the 'rootfs' share configured?"

# Always set up an overlayfs writable layer on top of the read-only rootfs.
# The VirtioFS mount is read-only; containers need a writable root.
/bin/busybox mount -t tmpfs tmpfs /mnt/overlay-work
/bin/busybox mkdir -p /mnt/overlay-work/upper /mnt/overlay-work/work /mnt/merged
/bin/busybox mount -t overlay overlay \
    -o lowerdir=/mnt/rootfs,upperdir=/mnt/overlay-work/upper,workdir=/mnt/overlay-work/work \
    /mnt/merged || fatal "failed to mount overlayfs"
ROOTFS=/mnt/merged

# Prepare the new root
/bin/busybox mkdir -p "$ROOTFS/proc" "$ROOTFS/sys" "$ROOTFS/dev" "$ROOTFS/tmp"
/bin/busybox mount -t proc proc "$ROOTFS/proc"
/bin/busybox mount -t sysfs sysfs "$ROOTFS/sys"
/bin/busybox mount -t devtmpfs devtmpfs "$ROOTFS/dev"
/bin/busybox mount -t tmpfs tmpfs "$ROOTFS/tmp"

# Copy guest agent into the new root (it's in the initramfs, not the container image)
/bin/busybox mkdir -p "$ROOTFS/usr/local/bin"
/bin/busybox cp /usr/bin/vz-guest-agent "$ROOTFS/usr/local/bin/vz-guest-agent"

# Copy DNS config into the new root (initramfs resolv.conf is hidden after switch_root)
/bin/busybox mkdir -p "$ROOTFS/etc"
if [ ! -f "$ROOTFS/etc/resolv.conf" ]; then
    /bin/busybox cp /etc/resolv.conf "$ROOTFS/etc/resolv.conf"
fi

# Configure networking if a virtio-net device exists
if [ -d /sys/class/net/eth0 ]; then
    /bin/busybox ip link set eth0 up
    # Use the bundled udhcpc default script for proper DHCP lease handling
    /bin/busybox udhcpc -i eth0 -s /etc/udhcpc.script -q 2>/dev/null || true
fi

# Set hostname
/bin/busybox hostname container

# Start the guest agent in the new root (before switch_root, so it's ready ASAP)
/bin/busybox chroot "$ROOTFS" /usr/local/bin/vz-guest-agent --port 7424 &

# Switch into the container rootfs using switch_root.
# switch_root deletes everything on the initramfs before switching,
# which is the standard approach for initramfs-to-rootfs transitions.
# The ~15 MB initramfs memory is reclaimed.
# Note: the guest agent is already running in the new root's context.
exec /bin/busybox switch_root "$ROOTFS" /bin/sh -c '
    # PID 1 just waits — keeps the VM alive.
    # The guest agent handles all commands via Exec requests from the host.
    # There is no kernel cmdline entrypoint — the host sends the entrypoint
    # as an Exec request after the agent responds to the Handshake.
    wait
'
```

### /etc/udhcpc.script

A minimal DHCP callback script included in the initramfs:

```bash
#!/bin/busybox sh
# Minimal udhcpc callback script for Virtualization.framework NAT
case "$1" in
    bound|renew)
        /bin/busybox ip addr add "$ip/$mask" dev "$interface"
        if [ -n "$router" ]; then
            /bin/busybox ip route add default via "$router"
        fi
        if [ -n "$dns" ]; then
            echo "nameserver $dns" > /etc/resolv.conf
        fi
        ;;
esac
```

### Build

```bash
# Create initramfs directory structure
mkdir -p initramfs/{bin,usr/bin,etc,dev,proc,sys,tmp}
mkdir -p initramfs/mnt/{rootfs,overlay-work,merged}

# Install busybox (statically linked arm64)
cp busybox-arm64-static initramfs/bin/busybox
chmod +x initramfs/bin/busybox

# Create busybox symlinks
for cmd in sh mount umount mkdir cp cat ls ip hostname chroot switch_root udhcpc; do
    ln -s busybox initramfs/bin/$cmd
done

# Install udhcpc callback script
cp udhcpc.script initramfs/etc/udhcpc.script
chmod +x initramfs/etc/udhcpc.script

# Install guest agent (compiled for aarch64-unknown-linux-musl)
cargo build --release -p vz-guest-agent --target aarch64-unknown-linux-musl
cp target/aarch64-unknown-linux-musl/release/vz-guest-agent initramfs/usr/bin/

# Install init script
cp init.sh initramfs/init
chmod +x initramfs/init

# Create the initramfs cpio archive
cd initramfs && find . | cpio -o -H newc | gzip > ../initramfs.img
```

The guest agent must be statically linked (`musl` target) since the initramfs has no dynamic linker. The container rootfs may have its own libc, but the agent is copied into the new root and started before switch_root.

## Guest Agent (Linux)

The same `vz-guest-agent` binary used in macOS VMs, compiled for `aarch64-unknown-linux-musl`. Same vsock protocol, same wire format, same handshake.

Differences from macOS:

| Aspect | macOS Guest Agent | Linux Guest Agent |
|--------|-------------------|-------------------|
| Build target | `aarch64-apple-darwin` | `aarch64-unknown-linux-musl` |
| Linking | Dynamic (system libc) | Static (musl) |
| Service manager | launchd | Started by /init script |
| User switching | `launchctl asuser` | `su -c` or `setuid`/`setgid` |
| System info | `sysctl hw.memsize`, `sw_vers` | `/proc/meminfo`, `uname -r` |
| AF_VSOCK | Available in macOS kernel | Requires `CONFIG_VSOCKETS` in kernel |

The protocol is identical. The host doesn't need to know whether it's talking to a macOS or Linux guest agent — the handshake includes an `os` field (`"linux"` or `"macos"`) which the host can use if it cares. See `05-base-prerequisites.md` for the `HandshakeAck` change.

## LinuxVm API

```rust
pub struct LinuxVmConfig {
    /// Path to the Linux kernel image
    pub kernel: PathBuf,

    /// Path to the initramfs
    pub initramfs: PathBuf,

    /// Kernel command line arguments
    /// Default: "console=hvc0 quiet"
    pub cmdline: String,

    /// CPU cores (default: 2)
    pub cpus: u8,

    /// Memory in MB (default: 512 — Linux VMs need much less than macOS)
    pub memory_mb: u64,

    /// VirtioFS mounts (rootfs + optional bind mounts)
    pub shared_dirs: Vec<SharedDirConfig>,

    /// Enable vsock (default: true)
    pub vsock: bool,

    /// Enable networking (default: per container config)
    pub network: Option<NetworkConfig>,
}
```

The `LinuxVmConfig` maps to a `VmConfig` with `BootLoader::Linux`:

```rust
impl LinuxVmConfig {
    pub fn to_vm_config(&self) -> Result<VmConfig> {
        let mut builder = VmConfigBuilder::new()
            .cpus(self.cpus)
            .memory_bytes(self.memory_mb as u64 * 1024 * 1024)
            .boot_linux(
                self.kernel.clone(),
                Some(self.initramfs.clone()),
                self.cmdline.clone(),
            )
            .enable_vsock();

        for dir in &self.shared_dirs {
            builder = builder.shared_dir(dir.clone());
        }

        if let Some(ref net) = self.network {
            match net {
                NetworkConfig::Nat => { builder = builder.network_nat(); }
                NetworkConfig::Bridged { interface } => {
                    builder = builder.network_bridged(interface);
                }
            }
        }

        builder.build()
    }
}
```

## Boot Sequence Timeline

```
T=0.000s  VZLinuxBootLoader loads kernel + initramfs
T=0.100s  Kernel decompresses, initializes
T=0.300s  Kernel mounts initramfs as rootfs, runs /init
T=0.400s  /init mounts proc, sys, dev, VirtioFS
T=0.600s  /init does switch_root into container rootfs
T=0.700s  Guest agent starts, listens on vsock 7424
T=0.800s  Host sends Ping, receives Pong
T=0.900s  Container entrypoint starts
T<1.000s  Ready
```

Target: **kernel boot to guest-agent-reachable in under 1 second.** The container entrypoint is sent as an `Exec` request after the handshake completes, adding negligible overhead. Total cold-start (assuming image is cached) is <2 seconds including rootfs assembly, VirtioFS mount, overlayfs setup, and switch_root.

Compare to:
- Docker Desktop: ~5-10s (shared VM already running, container starts in ~1s)
- macOS VM cold boot: 30-60s
- macOS VM restore: 5-10s
- Firecracker on Linux: ~125ms (KVM, not available on macOS)

## No Disk Images

Unlike macOS VMs (which require a 64 GB disk image), Linux VMs have **no disk images**. The container's rootfs is shared from the host via VirtioFS. Writes go to a tmpfs overlay inside the VM.

This means:
- No disk creation step (fast)
- No disk space consumed per container (only the OCI image layers on the host, shared across containers)
- Container teardown is just stopping the VM (no disk cleanup)
- Multiple containers from the same image share the same read-only base layers

## Resource Defaults

Linux VMs need far fewer resources than macOS VMs:

| Resource | macOS VM (sandbox) | Linux VM (container) |
|----------|-------------------|---------------------|
| CPUs | 4 | 2 |
| Memory | 8 GB | 512 MB (default, configurable per-container) |
| Disk | 64 GB image | None (VirtioFS) |
| Boot time | 5-10s (restore) | <1s |
| VM limit | 2 concurrent | Unlimited (limited by host RAM) |

The "unlimited" Linux VM count is a major advantage. You can run dozens of containers concurrently, each in its own hardware-isolated VM, limited only by host RAM (~128 MB overhead per VM for the kernel + agent).

**Note:** The 2-VM kernel limit applies only to **macOS** guest VMs, not Linux guests. Apple's Virtualization.framework does not impose a limit on the number of concurrent Linux VMs. This should be verified empirically on the target macOS version during Phase 1 implementation. If a limit is discovered, it will be documented as a constraint.

## Kernel + Initramfs Lifecycle

### First Run

```
vz run ubuntu:24.04 -- echo hello
  │
  ├── Check ~/.vz/linux/vmlinux exists
  │   └── If not: extract from vz-cli bundle or download
  │
  ├── Check ~/.vz/linux/initramfs.img exists
  │   └── If not: extract from vz-cli bundle or download
  │
  ├── Pull ubuntu:24.04 (if not cached)
  ├── Unpack layers to ~/.vz/oci/layers/
  ├── Assemble rootfs
  ├── Boot Linux VM with rootfs via VirtioFS
  ├── Wait for guest agent (~1s)
  ├── Exec "echo hello"
  └── Stop VM
```

### Updates

When `vz` is updated, the kernel + initramfs may also update. The CLI checks the version embedded in the binary against what's installed at `~/.vz/linux/` and replaces if newer.

```
~/.vz/linux/
├── vmlinux                # arm64 Linux kernel
├── initramfs.img          # initramfs with guest agent
└── version.json           # { "kernel": "6.12", "agent": "0.2.0" }
```
