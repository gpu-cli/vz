# Linux Machine Target and Linux-Host Support

Linux is the universal `vz` Machine target. The product roadmap supports Linux
Machines inside Developer Environment topologies on macOS, Linux, and Windows:

| Host | Linux Machine status | Backend direction |
| --- | --- | --- |
| Apple Silicon macOS | <!-- capability-matrix: macos-arm64/linux/* pair -->**DEV** Linux Machines with installed local-Mac evidence; private Developer-profile Docker is <!-- capability-matrix: macos-arm64/linux/developer docker_engine,compose,buildx -->**DEV**; topology networking is <!-- capability-matrix: macos-arm64/linux/* network_private -->**PLANNED** | Apple Virtualization.framework |
| Linux | <!-- capability-matrix: linux-*/linux/* pair -->**PLANNED:** the partial `linux-native` OCI backend exists in the tree, but no Machine target resolves on a Linux host | Native namespaces and cgroup v2 |
| Windows | <!-- capability-matrix: windows-*/linux/* pair -->**PLANNED:** no backend yet | Windows virtualization appropriate for a Linux guest |

Labels follow `config/host-target-capabilities-v0.4.json`.

Native macOS Machine integration is also part of the immediate macOS roadmap;
native Windows Machines come later. Native Machines do not inherit Docker: it
is an implicit, private capability of each **Developer-profile Linux Machine**
only, not a Hardened Machine.

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

`VZ_BACKEND` is an implementation/testing control for callers that actually
construct that runtime backend. It does not add an OCI or VM command to the
public CLI. The old `vz oci run` examples and infrastructure command workflows
are not executable current onboarding. Use typed runtime APIs with explicit
target/ownership checks; see [retired workflows](retired-cli-workflows.md).

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
| Status | <!-- capability-matrix: backend:macos-vz pair -->DEV (installed local-Mac slices, not release certified) | <!-- capability-matrix: backend:linux_native pair -->PLANNED (experimental code, no Machine target resolver) |
| Isolation | Full VM (Virtualization.framework) | Namespaces + cgroups |
| Container runtime | Pinned youki inside VM | youki on host today; pinned artifact integration planned |
| Networking | Guest agent + vsock bridge | Linux bridge + veth pairs |
| Port forwarding | Guest agent NAT | iptables DNAT |
| Service discovery | Declared Machine-local identities | Declared Machine-local identities |
| Host imports | Explicit authenticated relay planned; undeclared access is default-deny | Explicit authenticated relay planned; undeclared access is default-deny |
| Filesystem | VirtioFS | Direct host filesystem |
| Resource limits (CPU) | VM-level CPU count | cgroup v2 `cpu.max` |
| Resource limits (memory) | VM-level memory | cgroup v2 `memory.max` (planned) |

The end-state is one Environment topology model despite different host backends.
Every Developer-profile Linux Machine owns its target state and, once Docker compatibility lands,
its own Docker Engine, containerd, BuildKit cache, image store, volumes,
networks, endpoint, and context. No host uses an Environment-global or global
`vz` daemon, and a Machine never falls back to a sibling, another Environment,
system Docker, or Docker Desktop.

Host access is separate from service discovery and external egress. A Linux
Machine without a declared host import must not receive `host.vz.internal` or
another implicit gateway route. Binding a host service to a wildcard or LAN
address is not a supported substitute for the planned exact host-loopback,
authenticated import relay.

## Known Limitations

1. **Image pull is not yet integrated** -- the Linux-native backend currently expects
   a local rootfs directory path as the "image" reference. Full registry pull support
   requires wiring the OCI image store (tracked separately).

2. **Rootful networking** -- bridge/veth/iptables operations require root or
   `CAP_NET_ADMIN`. Rootless networking (e.g., via slirp4netns) is not yet supported.

3. **No GPU passthrough** -- containers run without GPU access on both backends.

4. **No persistent stack state** -- if the vz process exits, in-memory stack state
   (bridge names, netns tracking) is lost. Containers remain running but port forwarding
   rules and bridge interfaces may become orphaned. Teardown through the topology-scoped
   typed API before exiting; the legacy stack CLI is retired.

5. **Developer Environment parity is incomplete on Linux hosts** -- the current
   topology CLI exposes all five <!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV** lifecycle verbs,
   including `delete`, but complete Up reconciliation remains unfinished.
   Physical Up, execution, Stop and Delete adapters currently target Linux and
   native macOS Machines on Apple silicon, not the Linux host backend.
   An unsupported backend fails explicitly. Retired `init`, `run`, VM, image,
   and debug families are absent on all hosts, not macOS-only alternatives.

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
