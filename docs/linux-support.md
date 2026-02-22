# Linux Native Support

vz supports running OCI containers directly on Linux hosts without a VM layer,
using the `linux-native` backend.

## Compatibility Matrix

| Distribution       | Support Tier | Notes                                   |
|--------------------|-------------|-----------------------------------------|
| Ubuntu 22.04+ LTS  | Primary     | cgroup v2 by default, well-tested       |
| Debian 12+         | Primary     | cgroup v2 by default                    |
| Fedora 38+         | Primary     | cgroup v2 by default                    |
| Arch Linux         | Best effort | Rolling release, generally works        |
| RHEL/CentOS 9+     | Best effort | cgroup v2 available, may need enablement|

## Requirements

### Kernel Features

- **cgroup v2** (unified hierarchy) -- required for resource limits and container isolation
- **User namespaces** -- required for rootless container execution
- **Network namespaces** -- required for inter-service networking in stacks

### Runtime Dependencies

An OCI-compliant container runtime must be installed:

| Runtime | Install                                              |
|---------|------------------------------------------------------|
| youki   | `cargo install youki` or download from GitHub releases|
| runc    | `apt install runc` / `dnf install runc`              |

### Networking (for stacks)

Multi-service stacks require:
- `ip` command (from `iproute2` package) -- bridge/veth/netns management
- `iptables` -- port forwarding (DNAT) and NAT masquerade
- Root or `CAP_NET_ADMIN` -- for network namespace and bridge operations

## Backend Selection

The backend is selected automatically based on the host OS:
- macOS -> `macos-vz` (Virtualization.framework)
- Linux -> `linux-native`

Override with the `VZ_BACKEND` environment variable:

```bash
VZ_BACKEND=linux-native vz oci run alpine:latest -- echo ok
VZ_BACKEND=macos-vz    vz oci run alpine:latest -- echo ok
```

Accepted values: `linux`, `linux-native`, `native`, `macos`, `macos-vz`, `vm`.

## Capability Probes

Run capability probes to check if the host meets requirements:

```rust
use vz_linux_native::probe_host;

let report = probe_host();
if !report.all_satisfied() {
    eprintln!("Missing capabilities:\n{}", report.summary());
}
```

Probes check:
- `cgroup-v2`: `/sys/fs/cgroup/cgroup.controllers` exists
- `user-namespaces`: `/proc/sys/kernel/unprivileged_userns_clone` is `1` (or absent)
- `oci-runtime`: `youki` or `runc` found on `$PATH`

## Architecture Differences

| Feature                  | macOS (`macos-vz`)            | Linux (`linux-native`)         |
|--------------------------|-------------------------------|--------------------------------|
| Isolation                | Full VM (Virtualization.fw)   | Namespaces + cgroups           |
| Container runtime        | youki inside VM               | youki/runc on host             |
| Networking               | Guest agent + vsock bridge    | Linux bridge + veth pairs      |
| Port forwarding          | Guest agent NAT               | iptables DNAT                  |
| Service discovery        | /etc/hosts injection          | /etc/hosts injection           |
| Filesystem               | VirtioFS                      | Direct host filesystem         |
| Resource limits (CPU)    | VM-level CPU count            | cgroup v2 cpu.max              |
| Resource limits (memory) | VM-level memory               | cgroup v2 memory.max (planned) |

## Known Limitations

1. **Image pull is not yet integrated** -- the Linux-native backend currently expects
   a local rootfs directory path as the "image" reference. Full registry pull support
   requires wiring the OCI image store (tracked separately).

2. **Rootful networking** -- bridge/veth/iptables operations require root or
   `CAP_NET_ADMIN`. Rootless networking (e.g., via slirp4netns) is not yet supported.

3. **No GPU passthrough** -- containers run without GPU access on both backends.

4. **No persistent stack state** -- if the vz process exits, in-memory stack state
   (bridge names, netns tracking) is lost. Containers remain running but port forwarding
   rules and bridge interfaces may become orphaned. Run `vz stack down` before exiting.

5. **macOS-only commands unavailable** -- `vz init`, `vz run`, `vz exec`, `vz save`,
   `vz restore`, `vz list`, `vz stop`, `vz cache`, `vz provision`, `vz cleanup`,
   `vz self-sign`, and `vz validate` are only available on macOS.

## Troubleshooting

### "cgroup v2 unified hierarchy not found"

Your kernel is using cgroup v1. Enable cgroup v2:

```bash
# Check current cgroup version
stat -fc %T /sys/fs/cgroup/

# Enable cgroup v2 (add to kernel boot params)
# For GRUB: edit /etc/default/grub, add to GRUB_CMDLINE_LINUX:
systemd.unified_cgroup_hierarchy=1
# Then: sudo update-grub && sudo reboot
```

### "unprivileged user namespaces disabled"

```bash
sudo sysctl -w kernel.unprivileged_userns_clone=1

# Persist across reboots:
echo 'kernel.unprivileged_userns_clone=1' | sudo tee /etc/sysctl.d/99-userns.conf
sudo sysctl --system
```

### "no OCI runtime found"

Install youki or runc:

```bash
# youki (Rust-native, recommended)
cargo install youki

# runc (C, widely available)
sudo apt install runc        # Debian/Ubuntu
sudo dnf install runc        # Fedora
sudo pacman -S runc          # Arch
```

### Orphaned network resources after crash

If vz exits without cleanup, bridges and netns may remain:

```bash
# List vz bridges
ip link show type bridge | grep vz-

# Delete a specific bridge
sudo ip link del vz-abcdef01

# List vz network namespaces
ip netns list | grep vz-

# Delete a specific netns
sudo ip netns del vz-stackid-servicename

# Clean up iptables DNAT rules
sudo iptables -t nat -L PREROUTING -n --line-numbers | grep DNAT
sudo iptables -t nat -D PREROUTING <line-number>
```
