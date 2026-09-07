# Network increment plan: exports, imports, private paths, offline/allowed egress

Status: implementation plan (2026-09-07). Normative source:
[`GOAL-0.4.0.md`](GOAL-0.4.0.md) required-implementation items 5 and 6 and
acceptance criteria 5, 6, 7, 8, 9, 20. Tracked under `vz-mzs.5`.

## Current state (verified)

- Every Linux Machine gets one NAT NIC. `VmConfigBuilder::new()` defaults to
  `NetworkConfig::Nat` (`crates/vz/src/config.rs:110`); `LinuxVmConfig` only
  overrides when `Some` (`crates/vz-linux/src/config.rs:220-222, 296`); the
  Machine boot path sets `NetworkConfig::None` only when the default network is
  disabled (`crates/vz-oci-macos/src/runtime/stack_vm.rs:1673-1675`). The
  attachment is `VZNATNetworkDeviceAttachment` with a pinned MAC
  (`crates/vz/src/bridge.rs:406-442`). No vmnet or file-handle attachment is
  used anywhere, though `VZFileHandleNetworkDeviceAttachment` exists in the
  pinned `objc2-virtualization`.
- Native macOS Machines are created with no NIC at all
  (`crates/vz-runtimed/src/native_macos/runtime.rs:90`).
- The guest brings up `lo` then `eth0` by DHCP (`linux/initramfs/init:193-200`,
  `linux/initramfs/etc/udhcpc.script:5-9`). The Developer initramfs stages
  iptables (`linux/initramfs/init:84-100, 236`) and the kernel carries
  netfilter, bridge, veth, netns and vsock (`linux/vz-linux.config:11-84`), but
  no guest-agent code invokes iptables yet.
- Up rejects any declared network, endpoint or workspace projection
  (`crates/vz-runtimed/src/environment_up.rs:258-270`), and Stop and Delete
  carry the same allowlists (`environment_stop.rs:534-566`,
  `environment_delete.rs:645-678`).
- Typed `NetworkSpec` and `EndpointSpec` exist
  (`crates/vz-runtime-contract/src/types/topology.rs:233-274, 537-552`), with
  state-store tables (`crates/vz-stack/src/state_store/topology.rs:647-672`),
  but `MachineSpec` has no network attachment and no `HostImport`,
  `HostExport`, `EgressPolicy` or `PeerGrant` type exists.
- Docker traffic already relays Mac to guest over vsock
  (`machine_docker_endpoint.rs:128-146, 214-226` to
  `crates/vz-guest-agent/src/docker_forward.rs:58-163`), and a general TCP relay
  exists (`PortForward` in `crates/vz-agent-proto/proto/agent.proto:67-71`,
  guest handler `grpc_server.rs:3158-3240`, loopback-only host listener
  `crates/vz-oci-macos/src/runtime/networking.rs:60-175`) used only by the
  legacy path. dockerd listens on a guest Unix socket only
  (`crates/vz-guest-agent/src/docker.rs:588-600`), so `-p` binds inside the
  guest.

## Substrate decision

The contract forbids a shared NAT gateway address from being an authorization
boundary, and requires sibling Environments to be unable to resolve or route to
one another even with identical CIDRs.

| Option | Isolation | Verdict |
|---|---|---|
| File-handle attachment plus a per-Environment userspace switch owned by the runtime daemon | Host-enforced by construction; two Environments never share an L2 segment | Primary data plane |
| Keep Apple NAT and enforce with guest iptables | Weak: every VM shares `bridge100`, and root in a Developer guest can flush the rules | Rejected as the boundary; keep as defense in depth |
| Hybrid: switch for the private fabric, vsock relays for host import/export, NAT NIC attached only when egress is allowed | Strong for offline Machines, private paths, imports, exports and cross-Environment isolation | **Recommended** |

The hybrid is the smallest coherent step: imports and exports never touch a NIC,
so no gateway address can become an authorization path; private connectivity
exists only where the switch forwards it; `offline` means no NAT NIC exists at
all. The one honest weakness is that `allowed` Machines still share Apple's NAT
until switch-side NAT lands, which must be labeled DEV and recorded.

## Increment

1. **Typed records.** `MachineSpec.networks` and `MachineSpec.egress`;
   `EnvironmentSpec.host_exports` and `host_imports` (the host destination is
   always `127.0.0.1`, with no host address field by design); instances for
   attachments, exports, imports and egress; new owned-resource kinds. Every new
   durable table is a migration barrier, or the gate fails.
2. **Switch and private paths** (criterion 5, and the default-deny half of
   criterion 8). A `NetworkConfig::FileHandle` arm and multi-NIC configs; one
   switch task per network owning a datagram socket per Machine; forwarding only
   for declared endpoints, with a flow table for replies, anti-spoof checks and
   per-rule counters. Static guest addressing by MAC from the kernel command
   line; DHCP only on the egress NIC.
3. **Host exports and Docker published ports** (criterion 7 export half,
   `docker.network.published_ports`). A loopback-only listener per export
   relayed over the existing `PortForward` path, so exports work for offline
   Machines; declared fixed ports bind before boot so collisions fail closed; a
   per-Machine dynamic port partition keeps Docker's own allocations unique; a
   watcher mirrors container port bindings and refuses non-loopback host IPs.
4. **Host imports** (criterion 7 import half). A per-Machine relay address on
   the guest loopback, a guest-initiated vsock stream authorized by the
   accepting VM handle plus a per-import credential, and a host dial to exactly
   the stored `127.0.0.1` service. The guest never sends a destination.
5. **Egress offline versus allowed.** Offline attaches no NAT NIC at all;
   allowed attaches one and the agent confines it with an owner-scoped
   deny-first rule set, labeled DEV.

Deferred, with the substrate carrying them: split DNS, `.test` names, TLS
ingress and the NAT edge (the switch owns a gateway address it can answer for);
faults (per-path delay, loss, bandwidth and partition applied in the switch with
TTL receipts); peering grants (a switch-to-switch relay for one endpoint with an
expiry); and the exhaustive denial matrix (switch counters plus the existing
host inventories).

## Relation to the mounts suite

Host bind projection is not VirtioFS-based for Machines today: Up rejects any
workspace projection and the Machine VM carries only the rootfs, runtime-binary
and legacy volume shares (`stack_vm.rs:1633-1650`,
`linux/initramfs/init:246-290, 331-345`). `docker.storage.bind_mounts` therefore
needs the workspace projected in its declared mode plus path translation in the
Engine endpoint adapter, which is independent of this network increment and
belongs to `vz-mzs.5.1` and `vz-7ez`.

## Evidence

Unit: frame parser and policy engine on synthetic frames; two switches with
overlapping CIDRs forwarding nothing between them; export bind, collision and
cleanup; relay half-close and cancellation; import authorization failures; port
allocator; contract validation; state-store round trip and barrier inventory.

Installed, through the five verbs: `gate.network.private_topology_paths`,
`gate.isolation.cross_environment_peering` (default-deny half only, so the
scenario stays honestly FAIL until peering lands),
`gate.host.import_export_boundaries` including the listener inventory proving
only loopback binds, and the `netpolicy` Docker suite moving
`docker.network.published_ports` and `docker.network.cleanup` out of the
uncovered list.

## Risks

Userspace frame path throughput; stateless reply rules would allow source-port
spoofing, so the flow table is mandatory; `allowed` Machines share the Apple NAT
until switch-side NAT lands; Docker dynamic-port partitioning caps published
ports per Machine and explicit-port collisions can only be reported; native
macOS Machines need a DHCP and ARP responder before the mixed-target half of
criterion 5; `PortForwardOpen` currently accepts any target host
(`grpc_server.rs:3183-3190`) and must be hardened before exports are exposed.
