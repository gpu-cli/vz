# Linux Machine Target and Linux-Host Support

Linux is the universal `vz` Machine target. The product roadmap supports Linux
Machines inside Developer Environment topologies on macOS, Linux, and Windows:

| Host | Linux Machine status | Backend direction |
| --- | --- | --- |
| Apple Silicon macOS | **ACTIVE** Linux VM/OCI/BuildKit primitives; unified lifecycle and private Docker are **DEV** | Apple Virtualization.framework |
| Linux | **DEV:** the partial `linux-native` OCI backend exists, but complete Developer Environment parity is not shipped | Native namespaces and cgroup v2 |
| Windows | **PLANNED:** no shipped backend yet | Windows virtualization appropriate for a Linux guest |

Native macOS Machines are also immediate on macOS; native Windows Machines come
later. Native Machines do not inherit Docker: it is an implicit, private
capability of each **Linux Machine** only.

The sections below document the current experimental Linux-host backend. They
are not a declaration of full cross-host or Docker compatibility.

## Linux-host candidate distributions

These are the intended compatibility tiers for the Linux-host roadmap. Until
the complete Linux Developer Environment conformance suite passes, treat them
as targets rather than release certifications.

| Distribution | Intended tier | Notes |
| --- | --- | --- |
| Ubuntu 22.04+ LTS | Primary | cgroup v2 by default |
| Debian 12+ | Primary | cgroup v2 by default |
| Fedora 38+ | Primary | cgroup v2 by default |
| Arch Linux | Best effort | Rolling release |
| RHEL/CentOS 9+ | Best effort | cgroup v2 available, may need enablement |

## Requirements

### Kernel Features

- **cgroup v2** (unified hierarchy) -- required for resource limits and container isolation
- **User namespaces** -- required for rootless container execution
- **Network namespaces** -- required for inter-service networking in stacks

### Runtime dependency

The experimental backend requires youki. Released Developer Environment
artifacts pin and checksum their youki binary; source-tree Linux-host testing
may use a matching youki installed on the host. runc, crun, and undeclared OCI
runtime fallbacks are not supported.

| Runtime | Source-tree installation |
| --- | --- |
| youki | `cargo install youki` or download the pinned version from the youki release artifacts |

### Networking (for stacks)

Multi-service stacks require:
- `ip` command (from `iproute2` package) -- bridge/veth/netns management
- `iptables` -- port forwarding (DNAT) and NAT masquerade
- Root or `CAP_NET_ADMIN` -- for network namespace and bridge operations

## Current experimental backend selection

Current low-level OCI calls select a backend automatically based on the host
OS:
- macOS -> `macos-vz` (Virtualization.framework)
- Linux -> `linux-native`

Override with the `VZ_BACKEND` environment variable:

```bash
VZ_BACKEND=linux-native vz oci run alpine:latest -- echo ok
VZ_BACKEND=macos-vz    vz oci run alpine:latest -- echo ok
```

Accepted values: `linux`, `linux-native`, `native`, `macos`, `macos-vz`, `vm`.
This is an implementation control, not the intended Developer Environment UX;
environment creation should eventually select the host backend automatically.

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
- `oci-runtime`: a usable OCI runtime is visible on `$PATH`

For Developer Environment acceptance, that runtime must be the pinned youki.
A legacy probe recognizing another runtime does not make that runtime a
supported fallback.

## Architecture Differences

| Feature | macOS host, Linux target (`macos-vz`) | Linux host, Linux target (`linux-native`) |
| --- | --- | --- |
| Status | Shipped environment workflow; Docker parity in progress | Experimental/partial |
| Isolation | Full VM (Virtualization.framework) | Namespaces + cgroups |
| Container runtime | Pinned youki inside VM | youki on host today; pinned artifact integration planned |
| Networking | Guest agent + vsock bridge | Linux bridge + veth pairs |
| Port forwarding | Guest agent NAT | iptables DNAT |
| Service discovery | `/etc/hosts` injection | `/etc/hosts` injection |
| Filesystem | VirtioFS | Direct host filesystem |
| Resource limits (CPU) | VM-level CPU count | cgroup v2 `cpu.max` |
| Resource limits (memory) | VM-level memory | cgroup v2 `memory.max` (planned) |

The end-state is one Environment topology model despite different host backends.
Every Developer-profile Linux Machine owns its target state and, once Docker compatibility lands,
its own Docker Engine, containerd, BuildKit cache, image store, volumes,
networks, endpoint, and context. No host uses an Environment-global or global
`vz` daemon, and a Machine never falls back to a sibling, another Environment,
system Docker, or Docker Desktop.

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

5. **Developer Environment parity is incomplete on Linux hosts** -- `vz init`, `vz run`, `vz exec`, `vz save`,
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

Install the pinned youki version required by this checkout:

```bash
# youki is the only supported OCI runtime
cargo install youki
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
